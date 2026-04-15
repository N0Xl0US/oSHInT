"""
orchestrator/github_source.py — Lightweight GitHub datasource adapter.

Fetches public user metadata from the GitHub REST API and emits one
IdentityClaim for the target account when found.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from maigret.agent.report import IdentityClaim

logger = logging.getLogger(__name__)


class GitHubSource:
    """Query GitHub's public users API and convert results to IdentityClaim."""

    def __init__(self, request_timeout: int = 10) -> None:
        self._request_timeout = request_timeout

    async def search(
        self,
        username: str,
        min_confidence: float = 0.35,
    ) -> list[IdentityClaim]:
        target = username.strip()
        if not target:
            return []

        url = f"https://api.github.com/users/{target}"
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "maigret-osint/2.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        async with httpx.AsyncClient(timeout=self._request_timeout) as client:
            resp = await client.get(url, headers=headers)

        if resp.status_code == 404:
            logger.info("GitHubSource: username=%s not found", target)
            return []
        if resp.status_code == 403:
            raise RuntimeError("GitHub API rate-limited or forbidden (HTTP 403)")
        if resp.status_code >= 500:
            raise RuntimeError(f"GitHub API temporary failure (HTTP {resp.status_code})")
        if resp.status_code != 200:
            raise RuntimeError(f"GitHub API unexpected status: HTTP {resp.status_code}")

        profile = resp.json()
        confidence = self._score_profile(profile, target)
        if confidence < float(min_confidence):
            return []

        claim = IdentityClaim(
            platform="github",
            url=profile.get("html_url") or f"https://github.com/{target}",
            username=(profile.get("login") or target).lower(),
            email=profile.get("email"),
            confidence=round(confidence, 3),
            tier=self._to_tier(confidence),
            verified=True,
            source_tool="github",
            institutions=self._extract_institutions(profile),
        )

        logger.info(
            "GitHubSource: username=%s confidence=%.2f repos=%s followers=%s",
            target,
            claim.confidence,
            profile.get("public_repos", 0),
            profile.get("followers", 0),
        )

        # Publish raw claims to osint.raw.github.v1 (fire-and-forget)
        from maigret.events.raw_publisher import publish_raw_claims
        publish_raw_claims([claim], subject=target, source_tool="github")

        return [claim]

    @staticmethod
    def _extract_institutions(profile: dict[str, Any]) -> list[str]:
        company = (profile.get("company") or "").strip()
        if not company:
            return []
        if company.startswith("@"):
            company = company[1:]
        return [company] if company else []

    @staticmethod
    def _to_tier(confidence: float) -> str:
        if confidence >= 0.75:
            return "high"
        if confidence >= 0.45:
            return "medium"
        return "low"

    @staticmethod
    def _score_profile(profile: dict[str, Any], target: str) -> float:
        """
        Heuristic confidence for a discovered GitHub profile.

        Strongly biased to direct login match + account activity.
        """
        score = 0.45

        login = str(profile.get("login") or "").lower().strip()
        if login == target.lower():
            score += 0.20

        if (profile.get("type") or "").lower() == "user":
            score += 0.10

        if int(profile.get("public_repos") or 0) > 0:
            score += 0.08
        if int(profile.get("followers") or 0) > 0:
            score += 0.06
        if int(profile.get("following") or 0) > 0:
            score += 0.03

        if profile.get("name"):
            score += 0.03
        if profile.get("bio"):
            score += 0.03
        if profile.get("company"):
            score += 0.02

        created_at = profile.get("created_at")
        if isinstance(created_at, str):
            try:
                created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                account_age_days = (datetime.now(timezone.utc) - created_dt).days
                if account_age_days > 365:
                    score += 0.05
            except ValueError:
                pass

        return min(score, 0.95)
