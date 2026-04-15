"""
orchestrator/maigret_source.py — Adapter to invoke Maigret via HTTP and
produce IdentityClaim objects.

Mirrors the pattern from agent/tools.py (search_username tool) but
returns typed IdentityClaim objects instead of raw dicts.
"""

from __future__ import annotations

import logging
from typing import Any

from maigret.agent.agent_config import AgentConfig
from maigret.agent.client import OsintClient
from maigret.agent.report import IdentityClaim

logger = logging.getLogger(__name__)


class MaigretSource:
    """
    Thin adapter wrapping the Maigret FastAPI endpoint.

    Calls /search_username_filtered and converts scored profiles
    into IdentityClaim objects for the orchestrator.
    """

    def __init__(self, client: OsintClient | None = None) -> None:
        self._client = client

    async def search(
        self,
        username: str,
        min_confidence: float = 0.3,
    ) -> list[IdentityClaim]:
        """
        Run Maigret via the FastAPI server and return IdentityClaims.

        The server already runs PPF (run_ppf_async) on its side,
        so the returned profiles are post-PPF scored profiles.
        """
        client = self._client or OsintClient(AgentConfig())
        try:
            result = await client.search_username_filtered(
                username=username,
                # Handover v2 critical gap: verified claims must drive resolution.
                verify_http=True,
                min_confidence=min_confidence,
            )
        finally:
            # Close only if we created the client ourselves
            if self._client is None:
                await client.close()

        profiles: list[dict[str, Any]] = result.get("profiles", [])

        claims: list[IdentityClaim] = []
        for p in profiles:
            try:
                claims.append(IdentityClaim(
                    platform=p.get("platform", "unknown"),
                    url=p.get("url"),
                    username=p.get("username", username),
                    confidence=float(p.get("confidence", 0.0)),
                    tier=p.get("tier", "medium"),
                    verified=int(p.get("http_status", 0)) == 200,
                    source_tool="maigret",
                ))
            except Exception as exc:
                logger.warning("Skipping malformed Maigret profile: %s", exc)

        logger.info(
            "MaigretSource: username=%s claims=%d (from %d profiles)",
            username, len(claims), len(profiles),
        )
        return claims
