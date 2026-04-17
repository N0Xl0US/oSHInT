"""
orchestrator/orchestrator.py — IdentityOrchestrator: the 5-step pipeline.

Step 1: Fan-out — kick off Maigret, Academic, GitHubOctosuite, Blackbird,
                   SpiderFoot, Holehe in parallel
Step 2: Claim Collection — gather all IdentityClaim objects into one list
Step 3: Feed into Splink — pass unified claims to SplinkResolver
Step 4: Build Golden Records — assemble resolved identity objects
Step 5: Publish — emit to identity.resolved

The orchestrator does NOT:
    - Publish to any raw topic (sources own those)
    - Re-implement PPF, Splink, or any producer logic
    - Write to Neo4j (downstream consumer of identity.resolved)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from urllib.parse import urlsplit, urlunsplit
import uuid
from dataclasses import dataclass, field
from typing import Any

from maigret.agent.report import GoldenRecord, IdentityClaim
from maigret.kafka.producer import get_kafka_producer
from maigret.orchestrator.config import OrchestratorConfig, get_orchestrator_config
from maigret.orchestrator.blackbird_source import BlackbirdSource
from maigret.orchestrator.spiderfoot_source import SpiderFootSource
from maigret.orchestrator.holehe_source import HoleheSource
from maigret.orchestrator.maigret_source import MaigretSource
from maigret.resolver.golden import build_golden_records
from maigret.resolver.linker import SplinkResolver
from maigret.resolver.publisher import build_resolved_event

logger = logging.getLogger(__name__)


# ── Result model ─────────────────────────────────────────────────────────────


@dataclass
class SourceStats:
    """Per-source metadata from the fan-out step."""

    source: str
    claim_count: int = 0
    platforms: list[str] = field(default_factory=list)
    success: bool = True
    error: str | None = None
    elapsed_seconds: float = 0.0
    details: dict[str, Any] | list[dict[str, Any]] | None = None


@dataclass
class OrchestratorResult:
    """Full output of an orchestrator run."""

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    username: str = ""
    golden_records: list[GoldenRecord] = field(default_factory=list)
    total_claims: int = 0
    total_clusters: int = 0
    source_stats: list[SourceStats] = field(default_factory=list)
    source_plan: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    kafka_published: int = 0
    kafka_failed: int = 0

    def to_summary_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary for the agent LLM."""
        sources = []
        for s in self.source_stats:
            source_entry = {
                "source": s.source,
                "claims": s.claim_count,
                "platforms": s.platforms,
                "success": s.success,
                "error": s.error,
                "elapsed_seconds": round(s.elapsed_seconds, 2),
            }
            if s.details is not None:
                source_entry["details"] = s.details
            sources.append(source_entry)

        source_details = {
            s.source: s.details
            for s in self.source_stats
            if s.details is not None
        }

        return {
            "run_id": self.run_id,
            "username": self.username,
            "total_claims": self.total_claims,
            "total_clusters": self.total_clusters,
            "golden_records_count": len(self.golden_records),
            "sources": sources,
            "source_details": source_details,
            "source_plan": self.source_plan,
            "kafka": {
                "published": self.kafka_published,
                "failed": self.kafka_failed,
            },
            "errors": self.errors,
            "golden_records": [
                {
                    "entity_id": gr.entity_id,
                    "confidence_score": gr.confidence_score,
                    "resolution_method": gr.resolution_method,
                    "alias_count": len(gr.aliases),
                    "source_count": len(gr.sources),
                    "platforms": sorted({a.platform for a in gr.aliases if a.platform}),
                    "quarantined": gr.quality_flags.quarantined,
                }
                for gr in self.golden_records
            ],
        }


# ── Main Orchestrator ────────────────────────────────────────────────────────


class IdentityOrchestrator:
    """
    Fan-out / Resolve / Publish pipeline.

    Sources invoked in parallel:
        username → maigret, github_octosuite, blackbird
        email    → holehe, blackbird, spiderfoot
        name     → academic

    Usage::

        orchestrator = IdentityOrchestrator()
        result = await orchestrator.resolve_identity("johndoe", name="John Doe")
    """

    def __init__(self, config: OrchestratorConfig | None = None) -> None:
        self._config = config or get_orchestrator_config()

    async def resolve_identity(
        self,
        username: str | None = None,
        name: str | None = None,
        email: str | None = None,
        emails: list[str] | None = None,
        linkedin_url: str | None = None,
        institution: str | None = None,
    ) -> OrchestratorResult:
        """
        Run the full 5-step identity resolution pipeline.

        Args:
            username:     Explicit username (enables username-native sources).
            name:         Full name (used by Academic search).
            email:        Primary email hint.
            emails:       Additional email hints to search alongside primary email.
            institution:  Institution hint (optional — narrows Academic search)

        Returns:
            OrchestratorResult with golden records and operational metadata.
        """
        explicit_username = self._resolve_explicit_username(username)
        target_name = name.strip() if name and name.strip() else None
        target_emails = self._resolve_target_emails(
            username=username,
            email=email,
            emails=emails,
        )
        target_email = target_emails[0] if target_emails else None
        target_linkedin_url = self._resolve_linkedin_url(linkedin_url)

        if not explicit_username and not target_name and not target_emails and not target_linkedin_url:
            raise ValueError("Provide at least one of: username, name, email, or linkedin_url")

        sources_selected: list[str] = []
        if target_name:
            sources_selected.append("academic")
        if target_emails:
            sources_selected.append("holehe")
        if explicit_username or target_emails:
            sources_selected.append("blackbird")
        if target_emails:
            sources_selected.append("spiderfoot")
        if explicit_username:
            sources_selected.extend(["maigret", "github_octosuite"])
        if target_linkedin_url:
            sources_selected.append("linkedin")

        if target_linkedin_url and not explicit_username and not target_name and not target_email:
            mode = "linkedin_only"
        elif explicit_username and (target_name or target_email or target_linkedin_url):
            mode = "hybrid_explicit_username"
        elif explicit_username:
            mode = "explicit_username"
        elif target_name and (target_email or target_linkedin_url):
            mode = "name_email"
        elif target_name:
            mode = "name_only"
        elif target_linkedin_url:
            mode = "linkedin_only"
        else:
            mode = "email_only"

        subject = explicit_username or target_name or target_email or target_linkedin_url or ""
        if explicit_username and target_emails:
            blackbird_query_type = "username+email"
            blackbird_query_types = ["username", "email"]
        elif explicit_username:
            blackbird_query_type = "username"
            blackbird_query_types = ["username"]
        elif target_emails:
            blackbird_query_type = "email"
            blackbird_query_types = ["email"]
        else:
            blackbird_query_type = None
            blackbird_query_types: list[str] = []

        result = OrchestratorResult(username=subject)
        result.source_plan = {
            "mode": mode,
            "selected_sources": sources_selected,
            "holehe_enabled": bool(target_emails),
            "blackbird_enabled": bool(explicit_username or target_emails),
            "blackbird_query_type": blackbird_query_type,
            "blackbird_query_types": blackbird_query_types,
            "spiderfoot_enabled": bool(target_emails),
            "explicit_username": explicit_username,
            "name": target_name,
            "email": target_email,
            "emails": target_emails,
            "linkedin_enabled": bool(target_linkedin_url),
            "linkedin_url": target_linkedin_url,
        }

        logger.info(
            "Orchestrator: starting identity resolution for '%s' (name=%s emails=%s)",
            subject, target_name, target_emails,
        )

        # ── Step 1: Fan-out ──────────────────────────────────────────────
        all_claims, source_stats = await self._fan_out(
            explicit_username,
            target_name,
            target_emails,
            target_linkedin_url,
            institution,
            include_maigret=bool(explicit_username),
            include_academic=bool(target_name),
            include_github_octosuite=bool(explicit_username),
            include_blackbird=bool(explicit_username or target_emails),
            include_spiderfoot=bool(target_emails),
            include_linkedin=bool(target_linkedin_url),
        )
        result.source_stats = source_stats

        # ── Step 2: Claim Collection ─────────────────────────────────────
        result.total_claims = len(all_claims)
        logger.info("Orchestrator: collected %d total claims", len(all_claims))

        if not all_claims:
            result.errors.append("No claims collected from any source")
            logger.warning("Orchestrator: no claims — skipping resolution")
            return result

        # ── Step 3: Feed into Splink ─────────────────────────────────────
        try:
            resolver = SplinkResolver()
            clusters = resolver.resolve(all_claims)
            result.total_clusters = len(clusters)
            logger.info("Orchestrator: Splink produced %d clusters", len(clusters))
        except Exception as exc:
            err = f"Splink resolution failed: {exc}"
            result.errors.append(err)
            logger.error("Orchestrator: %s", err)
            return result

        # ── Step 4: Build Golden Records ─────────────────────────────────
        try:
            golden_records = build_golden_records(clusters)
            result.golden_records = golden_records
            logger.info(
                "Orchestrator: built %d golden records", len(golden_records),
            )
        except Exception as exc:
            err = f"Golden record assembly failed: {exc}"
            result.errors.append(err)
            logger.error("Orchestrator: %s", err)
            return result

        # ── Step 5: Publish to identity.resolved ─────────────────────────
        await self._publish(golden_records, result)

        logger.info(
            "Orchestrator: complete for '%s' — %d golden records, %d published",
            subject, len(golden_records), result.kafka_published,
        )

        return result

    def _resolve_explicit_username(
        self,
        username: str | None,
    ) -> str | None:
        """Normalize an explicit username; do not derive from name/email hints."""
        if username and username.strip():
            username_candidate = username.strip().lower()
            if "@" in username_candidate:
                return None
            username_candidate = re.sub(r"[^a-z0-9_]", "", username_candidate)
            if username_candidate:
                return username_candidate
        return None

    @staticmethod
    def _resolve_target_emails(
        username: str | None,
        email: str | None,
        emails: list[str] | None,
    ) -> list[str]:
        """Resolve all usable email hints for email-capable source invocation."""
        candidates: list[str] = []
        if email and "@" in email:
            candidates.append(email.strip().lower())

        if username and "@" in username:
            candidates.append(username.strip().lower())

        for item in emails or []:
            value = (item or "").strip().lower()
            if "@" in value:
                candidates.append(value)

        deduped: list[str] = []
        seen: set[str] = set()
        for value in candidates:
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)

        return deduped

    # ── Step 1: Fan-out (parallel source invocation) ─────────────────────────

    async def _fan_out(
        self,
        username: str | None,
        name: str | None,
        emails: list[str],
        linkedin_url: str | None,
        institution: str | None,
        include_maigret: bool,
        include_academic: bool,
        include_github_octosuite: bool,
        include_blackbird: bool,
        include_spiderfoot: bool,
        include_linkedin: bool,
    ) -> tuple[list[IdentityClaim], list[SourceStats]]:
        """
        Kick off all sources in parallel.

        Uses asyncio.gather with return_exceptions=True so a failure
        in one source never kills the rest.
        """
        tasks: list[Any] = []

        if include_maigret and username:
            tasks.append(
                self._run_source(
                    "maigret",
                    self._invoke_maigret(username),
                    self._effective_timeout("maigret", self._config.maigret_timeout),
                )
            )

        if include_academic and name:
            tasks.append(
                self._run_source(
                    "academic",
                    self._invoke_academic(name, institution),
                    self._config.academic_timeout,
                )
            )

        if include_github_octosuite and username:
            tasks.append(
                self._run_source(
                    "github_octosuite",
                    self._invoke_github_octosuite(username),
                    self._config.github_octosuite_timeout,
                )
            )

        if include_blackbird and (username or emails):
            tasks.append(
                self._run_source(
                    "blackbird",
                    self._invoke_blackbird(
                        username=username,
                        emails=emails,
                    ),
                    self._config.blackbird_timeout,
                )
            )

        if include_spiderfoot and emails:
            tasks.append(
                self._run_source(
                    "spiderfoot",
                    self._invoke_spiderfoot_many(emails),
                    self._config.spiderfoot_timeout,
                )
            )

        if include_linkedin and linkedin_url:
            tasks.append(
                self._run_source(
                    "linkedin",
                    self._invoke_linkedin(linkedin_url, email=emails[0] if emails else None),
                    self._effective_timeout("linkedin", self._config.linkedin_timeout),
                )
            )

        if emails:
            tasks.append(
                self._run_source(
                    "holehe",
                    self._invoke_holehe_many(emails),
                    self._config.holehe_timeout,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_claims: list[IdentityClaim] = []
        source_stats: list[SourceStats] = []

        for outcome in results:
            if isinstance(outcome, Exception):
                # This shouldn't happen since _run_source catches exceptions,
                # but handle it defensively.
                stat = SourceStats(source="unknown", success=False, error=str(outcome))
                source_stats.append(stat)
                logger.error("Orchestrator fan-out unexpected error: %s", outcome)
            else:
                claims, stat = outcome
                all_claims.extend(claims)
                source_stats.append(stat)

        return all_claims, source_stats

    async def _run_source(
        self,
        source_name: str,
        coro: Any,
        timeout: int,
    ) -> tuple[list[IdentityClaim], SourceStats]:
        """
        Run a single source with timeout and error isolation.

        Returns (claims, stats). Never raises.
        """
        stat = SourceStats(source=source_name)
        start = asyncio.get_event_loop().time()

        try:
            claims = await asyncio.wait_for(coro, timeout=timeout)
            stat.claim_count = len(claims)
            stat.platforms = sorted({c.platform for c in claims if getattr(c, "platform", "")})
            stat.details = self._extract_source_details(claims)
            stat.elapsed_seconds = asyncio.get_event_loop().time() - start
            logger.info(
                "Orchestrator: %s returned %d claims in %.1fs",
                source_name, len(claims), stat.elapsed_seconds,
            )
            return claims, stat

        except asyncio.TimeoutError:
            stat.success = False
            stat.error = f"Timed out after {timeout}s"
            stat.elapsed_seconds = asyncio.get_event_loop().time() - start
            logger.warning("Orchestrator: %s timed out after %ds", source_name, timeout)
            return [], stat

        except Exception as exc:
            stat.success = False
            stat.error = str(exc)
            stat.elapsed_seconds = asyncio.get_event_loop().time() - start
            logger.error("Orchestrator: %s failed — %s", source_name, exc)
            return [], stat

    def _effective_timeout(self, source_name: str, timeout: int) -> int:
        """
        Compute orchestrator timeout used by asyncio.wait_for for a source.

        Some source adapters already enforce their own internal timeout and may
        run cleanup/retry logic right at that boundary. Adding a small grace
        window prevents premature outer cancellation.
        """
        if source_name in {"maigret", "linkedin"}:
            grace = max(0, int(self._config.nested_timeout_grace_seconds))
            return timeout + grace
        return timeout

    @staticmethod
    def _extract_source_details(
        claims: list[IdentityClaim],
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        """Extract optional structured source payloads carried on claims."""
        payloads = [
            claim.raw_data
            for claim in claims
            if isinstance(getattr(claim, "raw_data", None), dict)
        ]
        if not payloads:
            return None
        if len(payloads) == 1:
            return payloads[0]
        return payloads

    # ── Source invokers ──────────────────────────────────────────────────────

    async def _invoke_maigret(self, username: str) -> list[IdentityClaim]:
        """Invoke Maigret via HTTP and return IdentityClaims."""
        source = MaigretSource()
        return await source.search(
            username=username,
            min_confidence=self._config.min_confidence_maigret,
        )

    async def _invoke_academic(
        self,
        name: str,
        institution: str | None = None,
    ) -> list[IdentityClaim]:
        """Invoke Academic search directly and return IdentityClaims."""
        from maigret.academic.runner import AcademicSearchTool

        tool = AcademicSearchTool()
        return await tool.search(
            username=name,
            institution=institution,
            min_confidence=self._config.min_confidence_academic,
        )

    async def _invoke_github_octosuite(self, username: str) -> list[IdentityClaim]:
        """
        Invoke the Octosuite-powered GitHub adapter and return IdentityClaims.

        Runs parallel sub-fetches (profile, repos, events, orgs) inside the
        adapter — the orchestrator treats this as a single async coroutine.
        Claims below min_confidence_github_octosuite are discarded here.

        The import is deferred so that a missing octosuite package does not
        prevent the orchestrator module from loading (the ImportError surfaces
        only when this source is actually invoked).
        """
        from maigret.orchestrator.github_octosuite_ppf import (
            GitHubOctosuitePPF,
            SubjectQuery as OctosuiteSubjectQuery,
        )

        adapter = GitHubOctosuitePPF()
        query = OctosuiteSubjectQuery(
            username=username,
            username_hint="github",
        )
        claims = await adapter.run(query)
        threshold = self._config.min_confidence_github_octosuite
        return [c for c in claims if c.confidence >= threshold]

    async def _invoke_blackbird(
        self,
        username: str | None = None,
        emails: list[str] | None = None,
    ) -> list[IdentityClaim]:
        """Invoke Blackbird datasource and return IdentityClaims."""
        source = BlackbirdSource(request_timeout=min(self._config.blackbird_timeout, 240))
        tasks: list[tuple[str, Any]] = []
        if username:
            tasks.append((
                "username",
                source.search_claims(
                    username=username,
                    min_confidence=self._config.min_confidence_blackbird,
                ),
            ))

        for email in emails or []:
            tasks.append((
                f"email:{email}",
                source.search_claims(
                    email=email,
                    min_confidence=self._config.min_confidence_blackbird,
                ),
            ))

        if not tasks:
            return []

        results = await asyncio.gather(
            *(task for _, task in tasks),
            return_exceptions=True,
        )

        merged: list[IdentityClaim] = []
        failed: list[str] = []
        for (kind, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                failed.append(f"{kind}: {result}")
                logger.warning("Blackbird %s query failed: %s", kind, result)
                continue
            merged.extend(result)

        if not merged and failed:
            raise RuntimeError("Blackbird queries failed: " + "; ".join(failed))

        deduped: list[IdentityClaim] = []
        seen: set[tuple[str, str, str | None, str | None]] = set()
        for claim in merged:
            key = (
                claim.platform,
                claim.username.lower(),
                claim.url,
                (claim.email or "").lower() or None,
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(claim)
        return deduped

    async def _invoke_spiderfoot(self, email: str) -> list[IdentityClaim]:
        """Invoke SpiderFoot datasource using email and return IdentityClaims."""
        source = SpiderFootSource(request_timeout=min(self._config.spiderfoot_timeout, 180))
        return await source.search(
            email=email,
            min_confidence=self._config.min_confidence_spiderfoot,
        )

    async def _invoke_spiderfoot_many(self, emails: list[str]) -> list[IdentityClaim]:
        """Invoke SpiderFoot for multiple emails and merge deduplicated claims."""
        return await self._invoke_many(emails, self._invoke_spiderfoot)

    async def _invoke_holehe(self, email: str) -> list[IdentityClaim]:
        """Invoke Holehe datasource and return IdentityClaims."""
        source = HoleheSource(request_timeout=min(self._config.holehe_timeout, 45))
        return await source.search(
            email=email,
            min_confidence=self._config.min_confidence_holehe,
        )

    async def _invoke_holehe_many(self, emails: list[str]) -> list[IdentityClaim]:
        """Invoke Holehe for multiple emails and merge deduplicated claims."""
        return await self._invoke_many(emails, self._invoke_holehe)

    async def _invoke_linkedin(
        self,
        linkedin_url: str,
        email: str | None = None,
    ) -> list[IdentityClaim]:
        """Invoke LinkedIn adapter using a direct profile URL."""
        from maigret.orchestrator.linkedin_ppf import LinkedInPPF, SubjectQuery

        adapter = LinkedInPPF()
        claims = await adapter.run(
            SubjectQuery(
                email=email,
                linkedin_url=linkedin_url,
            )
        )
        threshold = self._config.min_confidence_linkedin
        return [c for c in claims if c.confidence >= threshold]

    @staticmethod
    def _resolve_linkedin_url(linkedin_url: str | None) -> str | None:
        """Normalize LinkedIn URL input with adapter canonicalization when available."""
        candidate = (linkedin_url or "").strip()
        if not candidate:
            return None

        try:
            from maigret.orchestrator.linkedin_ppf import normalize_linkedin_profile_url
        except Exception:
            # Avoid hard dependency at orchestrator import time.
            return IdentityOrchestrator._normalize_linkedin_url_locally(candidate)

        return normalize_linkedin_profile_url(candidate)

    @staticmethod
    def _normalize_linkedin_url_locally(value: str) -> str | None:
        """Fallback LinkedIn profile URL normalization used when adapter import is unavailable."""
        try:
            parts = urlsplit(value)
        except Exception:
            return None

        if parts.scheme.lower() not in {"http", "https"}:
            return None

        host = parts.netloc.lower().split(":", 1)[0]
        if host.startswith("www."):
            host = host[4:]
        if host != "linkedin.com":
            return None

        segments = [seg for seg in (parts.path or "").split("/") if seg]
        if len(segments) < 2 or segments[0].lower() != "in":
            return None

        slug = segments[1].strip()
        if not slug:
            return None

        return urlunsplit(("https", "linkedin.com", f"/in/{slug}", "", ""))

    async def _invoke_many(
        self,
        emails: list[str],
        invoker: Any,
    ) -> list[IdentityClaim]:
        """Invoke an email-based source for each email and merge results."""
        if not emails:
            return []

        results = await asyncio.gather(
            *(invoker(email) for email in emails),
            return_exceptions=True,
        )

        merged: list[IdentityClaim] = []
        for email, result in zip(emails, results):
            if isinstance(result, Exception):
                logger.warning("Source invocation failed for %s: %s", email, result)
                continue
            merged.extend(result)

        deduped: list[IdentityClaim] = []
        seen: set[tuple[str, str, str | None, str | None]] = set()
        for claim in merged:
            key = (
                claim.platform,
                claim.username.lower(),
                claim.url,
                (claim.email or "").lower() or None,
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(claim)

        return deduped

    # ── Step 5: Publish ──────────────────────────────────────────────────────

    async def _publish(
        self,
        golden_records: list[GoldenRecord],
        result: OrchestratorResult,
    ) -> None:
        """
        Publish golden records to identity.resolved.

        Soft-fail: Kafka errors are logged and counted but never raised.
        """
        try:
            producer = get_kafka_producer()
        except Exception as exc:
            err = f"Cannot get Kafka producer: {exc}"
            result.errors.append(err)
            result.kafka_failed = len(golden_records)
            logger.warning("Orchestrator: %s — skipping publish", err)
            return

        for record in golden_records:
            try:
                payload_bytes, key = build_resolved_event(
                    record, trigger="orchestrator",
                )

                payload = json.loads(payload_bytes)
                payload["envelope"]["topic"] = self._config.resolved_topic
                payload_bytes = json.dumps(
                    payload, ensure_ascii=False,
                ).encode("utf-8")

                producer.publish(
                    payload_bytes,
                    key=key,
                    topic=self._config.resolved_topic,
                )
                result.kafka_published += 1

                logger.info(
                    "Orchestrator: published golden record %s to %s (conf=%.3f)",
                    record.entity_id,
                    self._config.resolved_topic,
                    record.confidence_score,
                )

            except Exception as exc:
                result.kafka_failed += 1
                result.errors.append(
                    f"Kafka publish failed for {record.entity_id}: {exc}",
                )
                logger.error(
                    "Orchestrator: Kafka publish failed for %s: %s",
                    record.entity_id, exc,
                )
