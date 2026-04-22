"""
orchestrator/github_octosuite_ppf.py — GitHub profile intelligence adapter via Octosuite.

Fetches public GitHub profile data (profile, repos, events, orgs) using the
octosuite library and emits a rich IdentityClaim with a structured raw_data
payload for downstream Splink entity resolution.

Adapter contract:
    adapter = GitHubOctosuitePPF()
    claims = await adapter.run(subject_query)

Kafka audit topic: osint.raw.github.v1
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, TypedDict
from uuid import uuid4

import httpx

from maigret.agent.report import IdentityClaim

logger = logging.getLogger("pipeline.github_octosuite")


def _resolve_octosuite_user_class() -> type[Any] | None:
    """
    Resolve an Octosuite User class across package layout variations.

    Some builds expose ``User`` at ``octosuite.User`` while others keep it in
    nested modules only (for example ``octosuite.api.models.User``).
    """
    candidates = [
        ("octosuite", "User"),
        ("octosuite.api.models", "User"),
        ("octosuite.models", "User"),
        ("octosuite.user", "User"),
    ]

    for module_name, attr_name in candidates:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue
        cls = getattr(module, attr_name, None)
        if cls is not None:
            return cls

    return None


_OCTOSUITE_USER_CLASS = _resolve_octosuite_user_class()

# ── Constants ─────────────────────────────────────────────────────────────────

_SOURCE = "github_octosuite"
_PLATFORM = "github"
_KAFKA_TOPIC = "osint.raw.github.v1"
_NEW_ACCOUNT_DAYS = 30
_EXECUTOR_SEMAPHORE_LIMIT = 3   # max concurrent thread-pool calls
_CONFIDENCE_BASE = 0.5
_CONFIDENCE_CAP = 1.0
_CONFIDENCE_FLOOR = 0.1
_LANGUAGE_ROUNDING_TOLERANCE = 1

# ── Typed sub-dicts (for IDE support / mypy) ─────────────────────────────────


class _ProfileDict(TypedDict, total=False):
    display_name: str | None
    location: str | None
    bio: str | None
    company: str | None
    blog: str | None
    public_repos: int
    followers: int
    following: int
    account_created_at: str | None
    account_updated_at: str | None


class _RepoDict(TypedDict, total=False):
    name: str
    description: str | None
    language: str | None
    topics: list[str]
    stars: int
    forks: int
    is_fork: bool
    created_at: str
    updated_at: str


class _PREventDict(TypedDict, total=False):
    target_repo: str
    pr_state: str
    pr_title: str | None
    created_at: str


class _OrgDict(TypedDict, total=False):
    login: str
    display_name: str | None


class _TechnicalProfileDict(TypedDict, total=False):
    primary_language: str | None
    active_repo_count: int
    org_count: int
    merged_pr_count: int
    external_pr_count: int


class _RawDataDict(TypedDict, total=False):
    profile: _ProfileDict
    repos: list[_RepoDict]
    pull_request_events: list[_PREventDict]
    orgs: list[_OrgDict]
    language_distribution: dict[str, int]
    technical_profile: _TechnicalProfileDict


# ── Minimal SubjectQuery shim ─────────────────────────────────────────────────
# The canonical SubjectQuery lives in pipeline.models (not yet available in this
# repo layout).  We define the interface locally so imports remain self-contained.
# If pipeline.models is present, its SubjectQuery is structurally compatible.

@dataclass
class SubjectQuery:
    """Minimal contract for the data passed to this adapter."""
    name: str | None = None
    email: str | None = None
    username: str | None = None
    username_hint: str | None = None


# ── Adapter ───────────────────────────────────────────────────────────────────


class GitHubOctosuitePPF:
    """
    GitHub profile intelligence adapter using the Octosuite library.

    Fetches profile, repos, PR events, and org memberships in parallel
    (executor-wrapped, semaphore-capped), runs the PPF, and returns a
    list containing at most one primary IdentityClaim.

    Kafka publish to osint.raw.github.v1 is fire-and-forget; failure
    is logged but never propagated.
    """

    def __init__(
        self,
        *,
        api_token: str | None = None,
        executor_semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        # Token from env or explicit injection (e.g. tests)
        self._token: str | None = api_token or os.getenv("GITHUB_API_TOKEN") or None
        self._sem = executor_semaphore or asyncio.Semaphore(_EXECUTOR_SEMAPHORE_LIMIT)
        self._user_class = _OCTOSUITE_USER_CLASS

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(self, subject_query: SubjectQuery) -> list[IdentityClaim]:
        """
        Execute the full fetch → PPF pipeline for a SubjectQuery.

        Returns a list with 0 or 1 IdentityClaim.
        Always returns an empty list on source unavailability —
        never raises.
        """
        username = (subject_query.username or "").strip()
        hint = (subject_query.username_hint or "").strip().lower()

        # Guard: username must be present
        if not username:
            logger.warning(
                "%s: SubjectQuery.username is absent — skipping fetch",
                _SOURCE,
            )
            return []

        # Guard: honour username_hint — only run when hint is "github" or absent
        if hint and hint != _PLATFORM:
            logger.info(
                "%s: username_hint=%r is not 'github' — skipping",
                _SOURCE, hint,
            )
            return []

        logger.info("%s: starting fetch for username=%r", _SOURCE, username)

        # ── Parallel fetches ──────────────────────────────────────────────
        user = self._build_user(username)

        profile_result, repos_result, events_result, orgs_result = (
            await asyncio.gather(
                self._fetch_profile(user, username),
                self._fetch_repos(user, username),
                self._fetch_events(user, username),
                self._fetch_orgs(user, username),
                return_exceptions=True,
            )
        )

        # ── Unpack results ────────────────────────────────────────────────
        profile = self._unwrap(profile_result, "profile", username)
        if profile is None:
            # Profile-not-found already logged inside fetch; nothing to claim
            return []

        repos: list[_RepoDict] = self._unwrap(repos_result, "repos", username) or []
        pr_events: list[_PREventDict] = self._unwrap(events_result, "events", username) or []
        orgs: list[_OrgDict] = self._unwrap(orgs_result, "orgs", username) or []

        # ── Assemble raw_data ─────────────────────────────────────────────
        raw_data = _build_raw_data(
            profile=profile,
            repos=repos,
            pr_events=pr_events,
            orgs=orgs,
            username=username,
        )

        # ── Kafka audit log (fire-and-forget) ─────────────────────────────
        try:
            self._publish_audit(username, raw_data)
        except Exception as _audit_exc:  # pragma: no cover
            logger.error(
                "%s: publish_audit raised unexpectedly for username=%r — %s",
                _SOURCE, username, _audit_exc,
            )

        # ── Build primary IdentityClaim ───────────────────────────────────
        claim = _build_claim(username=username, profile=profile, raw_data=raw_data)
        logger.info(
            "%s: claim built for username=%r confidence=%.3f needs_review=%s",
            _SOURCE, username, claim.confidence, claim.verified,
        )

        return [claim]

    # ── Octosuite user builder ────────────────────────────────────────────────

    def _build_user(self, username: str) -> Any:
        """Construct an Octosuite User when available, else use REST fallback client."""
        if self._user_class is not None:
            try:
                if self._token:
                    try:
                        user = self._user_class(username, token=self._token)
                    except TypeError:
                        # Some octosuite versions only accept a username arg.
                        user = self._user_class(username)
                else:
                    user = self._user_class(username)

                if self._supports_octosuite_contract(user):
                    return user

                logger.warning(
                    "%s: octosuite User object missing expected methods; falling back to REST adapter",
                    _SOURCE,
                )
            except Exception as exc:
                logger.warning(
                    "%s: octosuite User construction failed (%s); falling back to REST adapter",
                    _SOURCE,
                    exc,
                )
        else:
            logger.warning(
                "%s: octosuite User class unavailable; using REST adapter fallback",
                _SOURCE,
            )

        return _GitHubRestUser(username=username, token=self._token)

    @staticmethod
    def _supports_octosuite_contract(user: Any) -> bool:
        return all(
            callable(getattr(user, method_name, None))
            for method_name in ("exists", "repos", "events", "orgs")
        )

    # ── Executor-wrapped fetchers ─────────────────────────────────────────────

    async def _run_in_executor(self, fn: Any, *args: Any) -> Any:
        """
        Run a synchronous callable in the thread-pool executor.

        Semaphore caps concurrency to _EXECUTOR_SEMAPHORE_LIMIT to
        respect GitHub's unauthenticated rate limit (60 req/hr).
        """
        loop = asyncio.get_event_loop()
        async with self._sem:
            return await loop.run_in_executor(None, fn, *args)

    async def _fetch_profile(
        self, user: Any, username: str
    ) -> _ProfileDict | None:
        """
        Fetch user profile.

        Returns None if the user does not exist (404).
        Raises on rate-limit so the caller's gather(return_exceptions=True)
        can capture and log it.
        """
        raw: dict[str, Any] = await self._run_in_executor(
            _call_user_profile, user
        )

        if raw is None or raw.get("_not_found"):
            logger.info("%s: username=%r not found on GitHub", _SOURCE, username)
            return None

        if raw.get("_rate_limited"):
            raise _RateLimitedError(
                f"GitHub rate limit hit while fetching profile for {username!r}"
            )

        return _parse_profile(raw)

    async def _fetch_repos(
        self, user: Any, username: str
    ) -> list[_RepoDict]:
        """Fetch up to 100 public repos."""
        raw: list[dict[str, Any]] = await self._run_in_executor(
            _call_user_repos, user
        )
        if raw is None or isinstance(raw, _RateLimitedError):
            raise _RateLimitedError(
                f"GitHub rate limit hit while fetching repos for {username!r}"
            )
        return [_parse_repo(r) for r in raw if isinstance(r, dict)]

    async def _fetch_events(
        self, user: Any, username: str
    ) -> list[_PREventDict]:
        """Fetch up to 100 public events, filtering for PullRequestEvent only."""
        raw: list[dict[str, Any]] = await self._run_in_executor(
            _call_user_events, user
        )
        if raw is None:
            return []
        pr_events: list[_PREventDict] = []
        for evt in raw:
            if not isinstance(evt, dict):
                continue
            if str(evt.get("type", "")).strip() != "PullRequestEvent":
                continue
            parsed = _parse_pr_event(evt)
            if parsed is not None:
                pr_events.append(parsed)
        return pr_events

    async def _fetch_orgs(
        self, user: Any, username: str
    ) -> list[_OrgDict]:
        """Fetch up to 50 org memberships."""
        raw: list[dict[str, Any]] = await self._run_in_executor(
            _call_user_orgs, user
        )
        if raw is None:
            return []
        return [_parse_org(o) for o in raw if isinstance(o, dict)]

    # ── Result unwrapping ─────────────────────────────────────────────────────

    def _unwrap(
        self,
        result: Any,
        label: str,
        username: str,
    ) -> Any:
        """
        Unwrap a gather() result.

        • Exception → log, return None (sub-fetch skipped gracefully).
        • _RateLimitedError → log warning + set flag for caller.
        • Normal value → return as-is.
        """
        if isinstance(result, _RateLimitedError):
            logger.warning(
                "%s: rate limit hit during %s fetch for username=%r — "
                "returning partial data with needs_review=True",
                _SOURCE, label, username,
            )
            return None

        if isinstance(result, Exception):
            logger.error(
                "%s: unexpected error fetching %s for username=%r — %s",
                _SOURCE, label, username, result,
            )
            return None

        return result

    # ── Kafka audit publisher ─────────────────────────────────────────────────

    def _publish_audit(self, username: str, raw_data: _RawDataDict) -> None:
        """
        Publish the full pre-PPF payload to osint.raw.github.v1.

        Fire-and-forget: Kafka failures are logged and swallowed.
        """
        envelope: dict[str, Any] = {
            "subject_username": username,
            "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
            "source": _SOURCE,
            "payload": raw_data,
        }
        try:
            from maigret.kafka.producer import get_kafka_producer

            kafka = get_kafka_producer()
            payload_bytes = json.dumps(envelope, ensure_ascii=False, default=str).encode("utf-8")
            kafka.publish(payload_bytes, key=username, topic=_KAFKA_TOPIC)
            logger.info(
                "%s: audit event published | topic=%s username=%r bytes=%d",
                _SOURCE, _KAFKA_TOPIC, username, len(payload_bytes),
            )
        except Exception as exc:
            logger.error(
                "%s: Kafka audit publish failed | username=%r | %s",
                _SOURCE, username, exc,
            )


# ── Synchronous Octosuite call wrappers (run in thread executor) ──────────────
# These are plain functions — no async — intentionally, so they can be passed
# directly to run_in_executor.


class _GitHubRestUser:
    """Compatibility user object that mirrors the subset of Octosuite APIs we need."""

    def __init__(self, username: str, token: str | None = None) -> None:
        self._username = username
        self._token = token
        self._profile_cache: dict[str, Any] | None = None

        # Octosuite-like profile attributes populated after exists().
        self.name: str | None = None
        self.login: str | None = username
        self.location: str | None = None
        self.bio: str | None = None
        self.company: str | None = None
        self.blog: str | None = None
        self.public_repos: int = 0
        self.followers: int = 0
        self.following: int = 0
        self.created_at: str | None = None
        self.updated_at: str | None = None

    def exists(self) -> bool:
        try:
            profile = self._get_profile()
        except _GitHubNotFoundError:
            return False

        self._hydrate_profile_fields(profile)
        return True

    def repos(self, page: int = 1, per_page: int = 100) -> list[dict[str, Any]]:
        payload = self._request_json(
            f"/users/{self._username}/repos",
            params={"page": page, "per_page": per_page, "type": "owner", "sort": "updated"},
        )
        return payload if isinstance(payload, list) else []

    def events(self, page: int = 1, per_page: int = 100) -> list[dict[str, Any]]:
        payload = self._request_json(
            f"/users/{self._username}/events/public",
            params={"page": page, "per_page": per_page},
        )
        return payload if isinstance(payload, list) else []

    def orgs(self, page: int = 1, per_page: int = 50) -> list[dict[str, Any]]:
        payload = self._request_json(
            f"/users/{self._username}/orgs",
            params={"page": page, "per_page": per_page},
        )
        return payload if isinstance(payload, list) else []

    def _get_profile(self) -> dict[str, Any]:
        if self._profile_cache is None:
            payload = self._request_json(f"/users/{self._username}")
            if not isinstance(payload, dict):
                raise RuntimeError("GitHub profile response is not an object")
            self._profile_cache = payload
        return self._profile_cache

    def _hydrate_profile_fields(self, profile: dict[str, Any]) -> None:
        self.name = _clean_str(profile.get("name"))
        self.login = _clean_str(profile.get("login")) or self._username
        self.location = _clean_str(profile.get("location"))
        self.bio = _clean_str(profile.get("bio"))
        self.company = _clean_str(profile.get("company"))
        self.blog = _clean_str(profile.get("blog"))
        self.public_repos = int(profile.get("public_repos") or 0)
        self.followers = int(profile.get("followers") or 0)
        self.following = int(profile.get("following") or 0)
        self.created_at = _clean_str(profile.get("created_at"))
        self.updated_at = _clean_str(profile.get("updated_at"))

    def _request_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        headers: dict[str, str] = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "maigret-osint/2.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        url = f"https://api.github.com{path}"
        with httpx.Client(timeout=20) as client:
            response = client.get(url, params=params, headers=headers)

        if response.status_code == 404:
            raise _GitHubNotFoundError(f"GitHub resource not found (HTTP 404): {path}")
        if response.status_code in {403, 429}:
            raise _RateLimitedError(f"GitHub API rate limited (HTTP {response.status_code})")
        if response.status_code >= 500:
            raise RuntimeError(f"GitHub API temporary failure (HTTP {response.status_code})")
        if response.status_code != 200:
            raise RuntimeError(f"GitHub API unexpected status (HTTP {response.status_code})")

        return response.json()


def _call_user_profile(user: Any) -> dict[str, Any]:
    """
    Retrieve public profile data for the user.

    Returns a dict with sentinel key ``_not_found=True`` when the user
    does not exist, or ``_rate_limited=True`` on HTTP 403/429.
    """
    try:
        if not user.exists():
            return {"_not_found": True}
        # Octosuite's User object exposes profile attributes after exists()
        return _extract_octosuite_profile(user)
    except Exception as exc:
        msg = str(exc).lower()
        if any(code in msg for code in ("403", "429", "rate limit", "rate_limit", "forbidden")):
            return {"_rate_limited": True}
        if "404" in msg or "not found" in msg:
            return {"_not_found": True}
        raise


def _call_user_repos(user: Any) -> list[dict[str, Any]]:
    try:
        result = user.repos(page=1, per_page=100)
        return result if isinstance(result, list) else []
    except Exception as exc:
        msg = str(exc).lower()
        if any(code in msg for code in ("403", "429", "rate limit", "rate_limit", "forbidden")):
            raise _RateLimitedError(str(exc)) from exc
        logger.warning("%s: repos fetch error — %s", _SOURCE, exc)
        return []


def _call_user_events(user: Any) -> list[dict[str, Any]]:
    try:
        result = user.events(page=1, per_page=100)
        return result if isinstance(result, list) else []
    except Exception as exc:
        logger.warning("%s: events fetch error — %s", _SOURCE, exc)
        return []


def _call_user_orgs(user: Any) -> list[dict[str, Any]]:
    try:
        result = user.orgs(page=1, per_page=50)
        return result if isinstance(result, list) else []
    except Exception as exc:
        logger.warning("%s: orgs fetch error — %s", _SOURCE, exc)
        return []


# ── Octosuite attribute extraction ────────────────────────────────────────────


def _extract_octosuite_profile(user: Any) -> dict[str, Any]:
    """
    Pull all needed fields from an octosuite User instance.

    Octosuite exposes GitHub user fields as direct attributes after
    calling exists().  We access them defensively via getattr.
    """
    def _attr(name: str) -> Any:
        value = getattr(user, name, None)
        if callable(value):
            try:
                return value()
            except TypeError:
                return None
        return value

    return {
        "name":        _attr("name"),
        "login":       _attr("login"),
        "location":    _attr("location"),
        "bio":         _attr("bio"),
        "company":     _attr("company"),
        "blog":        _attr("blog"),
        "public_repos": int(_attr("public_repos") or 0),
        "followers":   int(_attr("followers") or 0),
        "following":   int(_attr("following") or 0),
        "created_at":  _attr("created_at"),
        "updated_at":  _attr("updated_at"),
    }


# ── Parsers ───────────────────────────────────────────────────────────────────


def _parse_profile(raw: dict[str, Any]) -> _ProfileDict:
    return {
        "display_name":      _clean_str(raw.get("name")),
        "location":          _clean_str(raw.get("location")),
        "bio":               _clean_str(raw.get("bio")),
        "company":           _clean_str(raw.get("company")),
        "blog":              _clean_str(raw.get("blog")),
        "public_repos":      int(raw.get("public_repos") or 0),
        "followers":         int(raw.get("followers") or 0),
        "following":         int(raw.get("following") or 0),
        "account_created_at": _clean_str(raw.get("created_at")),
        "account_updated_at": _clean_str(raw.get("updated_at")),
    }


def _parse_repo(raw: dict[str, Any]) -> _RepoDict:
    topics = raw.get("topics") or []
    if not isinstance(topics, list):
        topics = []
    return {
        "name":        str(raw.get("name") or ""),
        "description": _clean_str(raw.get("description")),
        "language":    _clean_str(raw.get("language")),
        "topics":      [str(t) for t in topics if t],
        "stars":       int(raw.get("stargazers_count") or raw.get("stars") or 0),
        "forks":       int(raw.get("forks_count") or raw.get("forks") or 0),
        "is_fork":     bool(raw.get("fork", False)),
        "created_at":  _clean_str(raw.get("created_at")) or "",
        "updated_at":  _clean_str(raw.get("updated_at")) or "",
    }


def _parse_pr_event(raw: dict[str, Any]) -> _PREventDict | None:
    """
    Extract a PR event.  Returns None if critical fields are absent.

    Octosuite / GitHub events API shape:
        {"type": "PullRequestEvent", "repo": {"name": "owner/repo"},
         "payload": {"action": "closed", "pull_request": {"merged": true, "title": "..."}},
         "created_at": "..."}
    """
    repo_name = _deep_get(raw, "repo", "name") or ""
    if not repo_name:
        return None

    payload = raw.get("payload") or {}
    pr = payload.get("pull_request") or {}
    action = str(payload.get("action") or "").lower()

    if action == "closed" and pr.get("merged"):
        pr_state = "merged"
    elif action == "closed":
        pr_state = "closed"
    else:
        pr_state = "open"

    return {
        "target_repo": repo_name,
        "pr_state":    pr_state,
        "pr_title":    _clean_str(pr.get("title")),
        "created_at":  _clean_str(raw.get("created_at")) or "",
    }


def _parse_org(raw: dict[str, Any]) -> _OrgDict:
    return {
        "login":        str(raw.get("login") or ""),
        "display_name": _clean_str(raw.get("name")),
    }


# ── raw_data assembly ─────────────────────────────────────────────────────────


def _build_raw_data(
    *,
    profile: _ProfileDict,
    repos: list[_RepoDict],
    pr_events: list[_PREventDict],
    orgs: list[_OrgDict],
    username: str,
) -> _RawDataDict:
    lang_dist = _compute_language_distribution(repos)
    tech = _compute_technical_profile(
        repos=repos,
        pr_events=pr_events,
        orgs=orgs,
        lang_dist=lang_dist,
        username=username,
    )
    return {
        "profile":              profile,
        "repos":                repos,
        "pull_request_events":  pr_events,
        "orgs":                 orgs,
        "language_distribution": lang_dist,
        "technical_profile":    tech,
    }


def _compute_language_distribution(repos: list[_RepoDict]) -> dict[str, int]:
    """
    Build language percentage distribution across non-fork repos.

    Rounded to the nearest integer; missing-language repos are excluded.
    Minor rounding drift within tolerance is absorbed into the top language.
    """
    non_fork_langs: list[str] = [
        r["language"]
        for r in repos
        if not r.get("is_fork") and r.get("language")
    ]
    if not non_fork_langs:
        return {}

    counts: Counter[str] = Counter(non_fork_langs)
    total = sum(counts.values())
    distribution = {
        lang: round(count * 100 / total)
        for lang, count in counts.most_common()
    }

    drift = 100 - sum(distribution.values())
    if distribution and drift and abs(drift) <= _LANGUAGE_ROUNDING_TOLERANCE:
        top_language = counts.most_common(1)[0][0]
        distribution[top_language] = max(0, distribution[top_language] + drift)

    return distribution


def _compute_technical_profile(
    *,
    repos: list[_RepoDict],
    pr_events: list[_PREventDict],
    orgs: list[_OrgDict],
    lang_dist: dict[str, int],
    username: str,
) -> _TechnicalProfileDict:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=365)

    active_count = 0
    for r in repos:
        updated = _parse_iso(r.get("updated_at", ""))
        if updated and updated >= cutoff:
            active_count += 1

    primary_lang: str | None = next(iter(lang_dist), None)

    merged_prs = sum(1 for e in pr_events if e.get("pr_state") == "merged")

    # External PR = PR to a repo whose owner segment differs from username
    external_prs = sum(
        1
        for e in pr_events
        if not (e.get("target_repo") or "").startswith(f"{username}/")
    )

    return {
        "primary_language":  primary_lang,
        "active_repo_count": active_count,
        "org_count":         len(orgs),
        "merged_pr_count":   merged_prs,
        "external_pr_count": external_prs,
    }


# ── IdentityClaim builder ─────────────────────────────────────────────────────


def _build_claim(
    *,
    username: str,
    profile: _ProfileDict,
    raw_data: _RawDataDict,
) -> IdentityClaim:
    display_name = profile.get("display_name")
    needs_review = _compute_needs_review(profile)
    confidence = _compute_confidence(profile, raw_data)

    # Map display_name → normalized_name representation.
    # The existing IdentityClaim Pydantic model does not have a
    # ``normalized_name`` field — we store it inside ``institutions``
    # which is a list[str] and is Splink-visible.  The display_name
    # itself is preserved verbatim in raw_data["profile"]["display_name"].
    normalized_name = _normalize_name(display_name)

    # Build institutions list: company membership surfaces to Splink
    institutions: list[str] = []
    company = profile.get("company")
    if company:
        clean = company.lstrip("@").strip()
        if clean:
            institutions.append(clean)

    return IdentityClaim(
        platform=_PLATFORM,
        url=f"https://github.com/{username}",
        username=username,
        email=None,
        confidence=round(confidence, 3),
        tier=_to_tier(confidence),
        # We repurpose ``verified`` as the inverse of needs_review so that
        # downstream Splink blocking correctly weights unreviewed claims lower.
        verified=not needs_review,
        source_tool=_SOURCE,
        institutions=institutions,
        # research_domains carries the primary language for Splink feature matching
        research_domains=(
            [raw_data["technical_profile"]["primary_language"]]
            if raw_data.get("technical_profile", {}).get("primary_language")
            else []
        ),
        raw_data=raw_data,
    )


# ── Confidence scoring ────────────────────────────────────────────────────────


def _compute_confidence(
    profile: _ProfileDict,
    raw_data: _RawDataDict,
) -> float:
    score = _CONFIDENCE_BASE

    if profile.get("display_name"):
        score += 0.15

    if profile.get("location"):
        score += 0.10

    if profile.get("company"):
        score += 0.10

    if int(profile.get("public_repos") or 0) >= 5:
        score += 0.05

    orgs = raw_data.get("orgs") or []
    if len(orgs) >= 1:
        score += 0.05

    # Penalise placeholder / bot accounts
    public_repos = int(profile.get("public_repos") or 0)
    followers = int(profile.get("followers") or 0)
    if public_repos == 0 and followers == 0:
        score -= 0.20

    return max(_CONFIDENCE_FLOOR, min(_CONFIDENCE_CAP, score))


def _compute_needs_review(profile: _ProfileDict) -> bool:
    # Trigger 1: no display name
    if not profile.get("display_name"):
        return True

    # Trigger 2: account created within the last 30 days
    created = _parse_iso(profile.get("account_created_at", ""))
    if created:
        age_days = (datetime.now(tz=timezone.utc) - created).days
        if age_days < _NEW_ACCOUNT_DAYS:
            return True

    # Trigger 3: 0 repos AND 0 followers
    public_repos = int(profile.get("public_repos") or 0)
    followers = int(profile.get("followers") or 0)
    if public_repos == 0 and followers == 0:
        return True

    return False


# ── Helpers ───────────────────────────────────────────────────────────────────


class _RateLimitedError(Exception):
    """Sentinel for GitHub 403/429 conditions inside executor threads."""


class _GitHubNotFoundError(Exception):
    """Sentinel for GitHub 404 responses in REST fallback adapter."""


def _clean_str(value: Any) -> str | None:
    """Strip and return None for blank/None values."""
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped if stripped else None


def _normalize_name(display_name: str | None) -> str | None:
    """Lowercase and whitespace-normalize a display name."""
    if not display_name:
        return None
    return " ".join(display_name.lower().split())


def _to_tier(confidence: float) -> str:
    if confidence >= 0.75:
        return "high"
    if confidence >= 0.45:
        return "medium"
    return "low"


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO8601 string to a timezone-aware datetime or return None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _deep_get(d: dict[str, Any], *keys: str) -> Any:
    """Safe nested dict accessor."""
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur
