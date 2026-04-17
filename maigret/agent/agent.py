"""
agent.py — InvestigationAgent: Groq-powered agentic OSINT investigator.

Orchestrates tool-use calls to the OSINT pipeline, reasons over results,
and synthesises an InvestigationReport with a partial GoldenRecord.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

from groq import Groq

from maigret.agent.agent_config import AgentConfig, get_agent_config
from maigret.agent.client import OsintClient
from maigret.agent.interfaces import (
    EntityResolver,
    GraphStore,
    InMemoryGraphStore,
    NoOpEntityResolver,
)
from maigret.agent.prompts import build_system_prompt, build_user_prompt
from maigret.agent.report import (
    Alias,
    GoldenRecord,
    GoldenRecordAttributes,
    IdentityClaim,
    InvestigationReport,
    QualityFlags,
    SourceRecord,
)
from maigret.agent.tools import ALL_TOOLS, execute_tool
from maigret.agent.tools import get_tools_for_context

logger = logging.getLogger(__name__)


class InvestigationAgent:
    """
    Groq-powered AI agent for OSINT background investigations.

    Usage:
        agent = InvestigationAgent()
        report = await agent.investigate("elonmusk")

    The agent:
    1. Sends a task + tool definitions to Groq
    2. Groq decides which tools to call (search_username, parse_url, etc.)
    3. Agent executes tools via HTTP against the OSINT server
    4. Results are fed back to Groq for reasoning
    5. Loop repeats until Groq responds with text or max rounds hit
    6. Final response is parsed into an InvestigationReport + partial GoldenRecord
    """

    def __init__(
        self,
        config: AgentConfig | None = None,
        graph_store: GraphStore | None = None,
        entity_resolver: EntityResolver | None = None,
    ) -> None:
        self._config = config or get_agent_config()
        self._groq = Groq(api_key=self._config.groq_api_key)
        self._graph_store = graph_store or InMemoryGraphStore()
        self._entity_resolver = entity_resolver or NoOpEntityResolver()

    # ── Public API ───────────────────────────────────────────────────────

    async def investigate(
        self,
        username: str | None = None,
        name: str | None = None,
        email: str | None = None,
        institution: str | None = None,
    ) -> InvestigationReport:
        """
        Run a full background investigation for a username.

        Returns an InvestigationReport with structured claims and a
        partial GoldenRecord.
        """
        if not username and not name and not email:
            raise ValueError("Provide at least one of: username, name, or email")

        subject = username or name or email or "unknown"
        logger.info("═══ Investigation started: %s ═══", subject)

        async with OsintClient(self._config) as client:
            explicit_username = username.strip() if username and username.strip() else None
            if explicit_username:
                query_plan = [explicit_username] + self._build_requery_candidates(explicit_username)
                query_plan = query_plan[: 1 + self._config.max_requery_attempts]
            else:
                query_plan = [None]

            total_tool_calls = 0
            total_searches: list[str] = []
            report: InvestigationReport | None = None

            for attempt_idx, query_username in enumerate(query_plan, start=1):
                if attempt_idx > 1 and query_username:
                    logger.info(
                        "Re-query attempt %d/%d with username variation: %s",
                        attempt_idx - 1,
                        self._config.max_requery_attempts,
                        query_username,
                    )

                messages, tool_calls_made, searches = await self._agentic_loop(
                    username=query_username,
                    name=name,
                    email=email,
                    institution=institution,
                    client=client,
                )
                total_tool_calls += tool_calls_made
                total_searches.extend(searches)

                candidate = self._parse_report(
                    username=subject,
                    messages=messages,
                    tool_calls_made=tool_calls_made,
                    searches=searches,
                )

                report = candidate
                if self._report_claim_count(candidate) > 0:
                    break

            if report is None:
                report = InvestigationReport(subject=subject, summary="No report produced")

            # Expose aggregate tool/search footprint across attempts.
            report.tool_calls_made = total_tool_calls
            report.search_history = total_searches

        # Store claims in graph (Phase 1: in-memory; Phase 2: Neo4j)
        all_claims = report.high_confidence + report.medium_confidence + report.low_confidence
        if all_claims:
            nodes_created = await self._graph_store.store_claims(subject, all_claims)
            logger.info("Stored %d claims in graph store", nodes_created)

        # Build partial Golden Record
        report.golden_record = self._build_golden_record(subject, report)

        logger.info(
            "═══ Investigation complete: %s | platforms=%d tool_calls=%d ═══",
            subject, report.platforms_found, report.tool_calls_made,
        )

        return report

    def _build_requery_candidates(self, username: str) -> list[str]:
        """Build deterministic username variations for one-shot follow-up retries."""
        base = username.strip().lower()
        if not base:
            return []

        candidates: list[str] = []

        compact = re.sub(r"[^a-z0-9]", "", base)
        if compact and compact != base:
            candidates.append(compact)

        no_digits = re.sub(r"\d+$", "", compact or base)
        if no_digits and no_digits not in {base, compact}:
            candidates.append(no_digits)

        # Preserve order while removing duplicates.
        deduped: list[str] = []
        for candidate in candidates:
            if candidate and candidate not in deduped:
                deduped.append(candidate)
        return deduped

    @staticmethod
    def _report_claim_count(report: InvestigationReport) -> int:
        return (
            len(report.high_confidence)
            + len(report.medium_confidence)
            + len(report.low_confidence)
        )

    # ── Agentic loop ─────────────────────────────────────────────────────

    async def _agentic_loop(
        self,
        username: str | None,
        name: str | None,
        email: str | None,
        institution: str | None,
        client: OsintClient,
    ) -> tuple[list[dict], int, list[str]]:
        """
        Core reasoning loop: LLM → tool_calls → execute → feed back → repeat.

        Returns (messages, total_tool_calls, searches_made).
        """
        messages: list[dict] = [
            {"role": "system", "content": build_system_prompt(self._config.max_total_searches)},
            {
                "role": "user",
                "content": build_user_prompt(
                    username=username,
                    name=name,
                    email=email,
                    institution=institution,
                ),
            },
        ]

        tools_for_context = get_tools_for_context(
            username=username,
            name=name,
            email=email,
        )

        tool_calls_made = 0
        searches: list[str] = []

        budget_tools = {
            "search_username",
            "search_academic",
            "search_github",
            "resolve_identity",
        }

        for round_num in range(1, self._config.max_tool_rounds + 1):
            logger.info("── Round %d/%d ──", round_num, self._config.max_tool_rounds)

            # Call Groq
            response = self._groq.chat.completions.create(
                model=self._config.groq_model,
                messages=messages,
                tools=tools_for_context,
                tool_choice="auto",
                temperature=self._config.groq_temperature,
                max_tokens=self._config.groq_max_tokens,
            )

            message = response.choices[0].message

            # If no tool calls → LLM is done reasoning, exit loop
            if not message.tool_calls:
                logger.info("LLM responded with text (no tool calls) — exiting loop")
                messages.append({"role": "assistant", "content": message.content or ""})
                break

            # Append the assistant's tool-call request
            messages.append({
                "role": "assistant",
                "content": message.content or "",
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in message.tool_calls
                ],
            })

            # Execute each tool call
            for tc in message.tool_calls:
                tool_name = tc.function.name
                try:
                    arguments = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    arguments = {}

                # Track search budget
                if tool_name in budget_tools:
                    tracked = (
                        arguments.get("username")
                        or arguments.get("name")
                        or arguments.get("email")
                        or tool_name
                    )
                    searches.append(str(tracked))
                    if len(searches) > self._config.max_total_searches:
                        logger.warning("Search budget exhausted (%d/%d)",
                                       len(searches), self._config.max_total_searches)
                        result_str = json.dumps({
                            "error": "Search budget exhausted. Please synthesise your findings now."
                        })
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": result_str,
                        })
                        tool_calls_made += 1
                        continue

                # Execute the tool
                result_str = await execute_tool(tool_name, arguments, client)
                tool_calls_made += 1

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result_str,
                })

                logger.info(
                    "Tool %s completed | result_length=%d",
                    tool_name, len(result_str),
                )

        else:
            # Max rounds exhausted — force a final response
            logger.warning("Max rounds (%d) exhausted — forcing final response",
                           self._config.max_tool_rounds)
            messages.append({
                "role": "user",
                "content": "You have used all your tool rounds. Please synthesise your findings now as the JSON report.",
            })
            response = self._groq.chat.completions.create(
                model=self._config.groq_model,
                messages=messages,
                temperature=self._config.groq_temperature,
                max_tokens=self._config.groq_max_tokens,
            )
            messages.append({
                "role": "assistant",
                "content": response.choices[0].message.content or "",
            })

        return messages, tool_calls_made, searches

    # ── Response parsing ─────────────────────────────────────────────────

    def _parse_report(
        self,
        username: str,
        messages: list[dict],
        tool_calls_made: int,
        searches: list[str],
    ) -> InvestigationReport:
        """Parse the final LLM message into an InvestigationReport."""
        # Find the last assistant message (the final report)
        final_text = ""
        for msg in reversed(messages):
            if msg.get("role") == "assistant" and msg.get("content"):
                final_text = msg["content"]
                break

        # Try to extract JSON from the response
        report_data = self._extract_json(final_text)

        if report_data:
            high = [IdentityClaim(**p) for p in report_data.get("high_confidence", [])]
            medium = [IdentityClaim(**p) for p in report_data.get("medium_confidence", [])]
            low = [IdentityClaim(**p) for p in report_data.get("low_confidence", [])]

            return InvestigationReport(
                subject=username,
                summary=report_data.get("summary", final_text[:500]),
                platforms_found=report_data.get("platforms_found", len(high) + len(medium) + len(low)),
                high_confidence=high,
                medium_confidence=medium,
                low_confidence=low,
                search_history=searches,
                tool_calls_made=tool_calls_made,
            )

        # Fallback: couldn't parse JSON — return the raw text as summary
        logger.warning("Could not parse structured JSON from LLM response — using raw text")
        return InvestigationReport(
            subject=username,
            summary=final_text[:1000] if final_text else "No response from LLM",
            search_history=searches,
            tool_calls_made=tool_calls_made,
        )

    def _extract_json(self, text: str) -> Optional[dict]:
        """Extract JSON from LLM response (handles markdown code fences)."""
        import re

        # Try: ```json ... ```
        match = re.search(r"```json\s*\n(.*?)\n\s*```", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # Try: ``` ... ```
        match = re.search(r"```\s*\n(.*?)\n\s*```", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # Try: raw JSON (find first { ... last })
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass

        return None

    # ── Golden Record builder ────────────────────────────────────────────

    def _build_golden_record(
        self,
        username: str,
        report: InvestigationReport,
    ) -> GoldenRecord:
        """
        Build a partial Golden Record from investigation results.

        Phase 1: deterministic, single-source (maigret only).
        Phase 2+: enriched by Splink and EvoKG.
        """
        all_claims = report.high_confidence + report.medium_confidence + report.low_confidence

        # Build aliases from claims
        aliases = [
            Alias(
                type="username",
                value=claim.username,
                platform=claim.platform,
                confidence=claim.confidence,
                source=claim.source_tool,
            )
            for claim in all_claims
        ]

        # Calculate overall confidence
        if all_claims:
            overall_confidence = sum(c.confidence for c in all_claims) / len(all_claims)
        else:
            overall_confidence = 0.0

        # Quality flags based on handover.md §1 thresholds
        config = self._config
        quality_flags = QualityFlags(
            low_confidence=overall_confidence < config.confidence_flag_ingest,
            conflicting_data=False,  # Phase 2: detected by EvoKG
            quarantined=overall_confidence < config.confidence_quarantine,
            needs_review=overall_confidence < config.confidence_direct_ingest,
        )

        return GoldenRecord(
            confidence_score=round(overall_confidence, 4),
            resolution_method="deterministic",
            aliases=aliases,
            attributes=GoldenRecordAttributes(
                name_variants=[username],
            ),
            sources=[
                SourceRecord(tool="maigret", reliability_weight=0.8),
            ],
            quality_flags=quality_flags,
        )
