"""
orchestrator/github_playwright_source.py — GitHub README enrichment via Playwright.

Uses the Playwright scraper to collect:
- GitHub account metadata
- Profile README (username/username)
- Repository READMEs (auto-discovered via GitHub REST API)

Emits one high-quality IdentityClaim for Splink, with rich raw_data payload.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from maigret.agent.report import IdentityClaim
from maigret.events.raw_publisher import publish_raw_claims

logger = logging.getLogger(__name__)


class GitHubPlaywrightSource:
    """Collect GitHub profile and README intelligence via Playwright."""

    def __init__(self, request_timeout: int = 45, max_repo_targets: int = 8) -> None:
        self._request_timeout = max(10, int(request_timeout))
        self._max_repo_targets = max(0, int(max_repo_targets))

    async def search(
        self,
        username: str,
        min_confidence: float = 0.35,
    ) -> list[IdentityClaim]:
        target = (username or "").strip()
        if not target:
            return []

        scraper = self._load_scraper()
        profile_url = f"https://github.com/{target}"

        repo_urls = await self._discover_repo_urls(target)
        scrape_input_urls = [profile_url, *repo_urls]

        result = await scraper(urls=scrape_input_urls, headless=True)
        profile_payload = self._pick_profile(result, target)
        if profile_payload is None:
            logger.info("GitHubPlaywrightSource: username=%s profile not found", target)
            return []

        repos_payload = result.get("repos") or []
        confidence = self._score(profile_payload=profile_payload, repos_payload=repos_payload)
        if confidence < float(min_confidence):
            return []

        profile_readme = profile_payload.get("profile_readme")
        account = profile_payload.get("account") or {}

        claim = IdentityClaim(
            platform="github",
            url=profile_payload.get("url") or profile_url,
            username=(profile_payload.get("username") or target).lower(),
            confidence=round(confidence, 3),
            tier=self._to_tier(confidence),
            verified=True,
            source_tool="github_playwright",
            institutions=self._extract_institutions(account),
            research_domains=self._infer_research_domains(repos_payload),
            raw_data={
                "profile": {
                    "url": profile_payload.get("url") or profile_url,
                    "username": profile_payload.get("username") or target,
                    "account": account,
                    "profile_readme": profile_readme,
                    "profile_readme_source": profile_payload.get("profile_readme_source"),
                },
                "repos": repos_payload,
                "stats": {
                    "repo_targets": len(repo_urls),
                    "repo_readmes_found": sum(1 for repo in repos_payload if (repo.get("readme") or "").strip()),
                    "source_errors": len(result.get("errors") or []),
                },
                "errors": result.get("errors") or [],
            },
        )

        publish_raw_claims([claim], subject=target, source_tool="github_playwright")
        logger.info(
            "GitHubPlaywrightSource: username=%s confidence=%.2f repos=%d",
            target,
            claim.confidence,
            len(repos_payload),
        )
        return [claim]

    def _load_scraper(self):
        """Lazy-import scraper to avoid import-time hard failure when Playwright is absent."""
        try:
            from github_playwright.github_pw import scrape_github_data

            return scrape_github_data
        except SystemExit as exc:
            raise RuntimeError(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Unable to load GitHub Playwright scraper: {exc}") from exc

    async def _discover_repo_urls(self, username: str) -> list[str]:
        """Discover candidate repos so README scraping can run beyond the profile page."""
        endpoint = f"https://api.github.com/users/{username}/repos"
        params = {
            "sort": "updated",
            "per_page": min(max(self._max_repo_targets, 1), 100),
            "type": "owner",
        }
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "maigret-osint/2.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        try:
            async with httpx.AsyncClient(timeout=self._request_timeout) as client:
                response = await client.get(endpoint, params=params, headers=headers)

            if response.status_code != 200:
                return []

            payload = response.json()
            if not isinstance(payload, list):
                return []

            urls: list[str] = []
            for item in payload:
                if not isinstance(item, dict):
                    continue
                html_url = str(item.get("html_url") or "").strip()
                if not html_url.startswith("https://github.com/"):
                    continue
                urls.append(html_url.rstrip("/"))
                if len(urls) >= self._max_repo_targets:
                    break

            return urls

        except Exception:  # noqa: BLE001 - non-fatal best effort discovery
            return []

    @staticmethod
    def _pick_profile(result: dict[str, Any], username: str) -> dict[str, Any] | None:
        profiles = result.get("profiles") or []
        if not isinstance(profiles, list):
            return None

        for profile in profiles:
            if not isinstance(profile, dict):
                continue
            if str(profile.get("username") or "").lower() == username.lower():
                return profile

        for profile in profiles:
            if isinstance(profile, dict):
                return profile

        return None

    @staticmethod
    def _extract_institutions(account: dict[str, Any]) -> list[str]:
        company = str(account.get("company") or "").strip()
        if not company:
            return []
        if company.startswith("@"):
            company = company[1:]
        return [company] if company else []

    @staticmethod
    def _infer_research_domains(repos_payload: list[dict[str, Any]]) -> list[str]:
        names: list[str] = []
        for repo in repos_payload:
            if not isinstance(repo, dict):
                continue
            repo_name = str(repo.get("repo") or "").strip()
            if repo_name:
                names.append(repo_name)
        return names[:5]

    @staticmethod
    def _score(profile_payload: dict[str, Any], repos_payload: list[dict[str, Any]]) -> float:
        score = 0.55

        account = profile_payload.get("account") or {}
        if account.get("display_name"):
            score += 0.06
        if account.get("followers"):
            try:
                if int(str(account.get("followers")).replace(",", "")) > 0:
                    score += 0.04
            except ValueError:
                pass

        if profile_payload.get("profile_readme"):
            score += 0.12

        repo_count = len(repos_payload)
        if repo_count > 0:
            score += 0.10

        repo_readme_count = sum(1 for repo in repos_payload if (repo.get("readme") or "").strip())
        if repo_readme_count > 0:
            score += 0.08

        return min(score, 0.95)

    @staticmethod
    def _to_tier(confidence: float) -> str:
        if confidence >= 0.75:
            return "high"
        if confidence >= 0.45:
            return "medium"
        return "low"
