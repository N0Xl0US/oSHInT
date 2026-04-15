"""
orchestrator/linkedin_ppf.py - LinkedIn profile intelligence adapter.

Primary input is a LinkedIn profile URL (linkedin.com/in/<handle>) and output
is a single IdentityClaim enriched with structured experience/education fields.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from types import SimpleNamespace

try:
    from linkedin_scraper import BrowserManager, PersonScraper, login_with_cookie  # type: ignore[import-untyped]
except ImportError as _exc:
    raise ImportError(
        "linkedin_scraper v3+ is required for LinkedInPPF. "
        "Install it with: pip install linkedin_scraper\n"
        f"Original error: {_exc}"
    ) from _exc

try:
    from pipeline.models import IdentityClaim, SubjectQuery  # type: ignore[import-not-found]
except ImportError:
    from maigret.agent.report import IdentityClaim

    # MIGRATION NOTE: Add linkedin_url: str | None = None to pipeline.models.SubjectQuery
    # in deployments that expose a shared pipeline.models contract.
    @dataclass(slots=True)
    class SubjectQuery:
        name: str | None = None
        email: str | None = None
        username: str | None = None
        username_hint: str | None = None
        linkedin_url: str | None = None


logger = logging.getLogger("pipeline.linkedin")

_SOURCE = "linkedin"
_PLATFORM = "linkedin"
_KAFKA_TOPIC = "osint.raw.linkedin.v1"


class LinkedInSessionMissingError(RuntimeError):
    """Raised when the persisted LinkedIn session cookie file is missing."""


class LinkedInSessionExpiredError(RuntimeError):
    """Raised when LinkedIn redirects to login/authwall during scrape."""

    def __init__(
        self,
        message: str,
        *,
        partial_claim: IdentityClaim | None = None,
    ) -> None:
        super().__init__(message)
        self.partial_claim = partial_claim


class LinkedInRateLimitReachedError(RuntimeError):
    """Raised when the configured daily scrape cap has been reached."""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_path(name: str, default: str) -> Path:
    return Path(os.getenv(name, default)).expanduser()


def _session_missing_message(path: Path) -> str:
    return (
        "LinkedIn session file is missing or unreadable at "
        f"'{path}'. Export your li_at browser cookie to JSON and set "
        "LINKEDIN_SESSION_PATH accordingly."
    )


def _session_invalid_message(path: Path) -> str:
    return (
        "LinkedIn session file at "
        f"'{path}' does not contain a valid li_at cookie. Replace the "
        "placeholder value with a real browser-exported li_at session cookie."
    )


def _session_cookie_from_env() -> str | None:
    raw = os.getenv("LINKEDIN_LI_AT")
    if raw is not None:
        value = raw.strip()
        return value or None

    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return None

    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.strip() != "LINKEDIN_LI_AT":
                continue
            candidate = value.strip().strip('"').strip("'")
            return candidate or None
    except Exception:
        return None

    return None


def _validate_session_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        raise LinkedInSessionMissingError(_session_missing_message(path))

    try:
        raw = path.read_text(encoding="utf-8")
    except Exception as exc:
        raise LinkedInSessionMissingError(_session_missing_message(path)) from exc

    try:
        payload = json.loads(raw)
    except Exception as exc:
        raise LinkedInSessionMissingError(_session_missing_message(path)) from exc

    if not isinstance(payload, dict) or "li_at" not in payload:
        raise LinkedInSessionMissingError(_session_missing_message(path))


def _validate_session_cookie(cookie: str, *, source_label: str = "environment") -> None:
    if not isinstance(cookie, str) or not cookie.strip() or cookie.strip().startswith("REPLACE_WITH_"):
        if source_label == "environment":
            raise LinkedInSessionMissingError(
                "LINKEDIN_LI_AT is missing or invalid. Set it to a real browser-exported li_at cookie."
            )
        raise LinkedInSessionMissingError(_session_invalid_message(Path("linkedin_session.json")))


_DEFAULT_SESSION_PATH = _env_path("LINKEDIN_SESSION_PATH", "./linkedin_session.json")
_SESSION_COOKIE_ENV = _session_cookie_from_env()
if _SESSION_COOKIE_ENV is None:
    _validate_session_file(_DEFAULT_SESSION_PATH)
else:
    _validate_session_cookie(_SESSION_COOKIE_ENV)


def normalize_linkedin_profile_url(value: str | None) -> str | None:
    """Return canonical https://linkedin.com/in/<slug> URL or None if invalid."""
    if not value:
        return None

    try:
        parts = urlsplit(value.strip())
    except Exception:
        return None

    if parts.scheme.lower() not in {"http", "https"}:
        return None

    host = parts.netloc.lower().split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    if host != "linkedin.com":
        return None

    path = re.sub(r"/+", "/", (parts.path or "").strip())
    segments = [seg for seg in path.split("/") if seg]
    if len(segments) < 2:
        return None
    if segments[0].lower() != "in":
        return None

    slug = segments[1].strip().strip(" |,.;:!?)]}>\"'")
    if not slug:
        return None

    canonical_path = f"/in/{slug}".rstrip("/")
    return urlunsplit(("https", "linkedin.com", canonical_path, "", ""))


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _normalize_name(value: str | None) -> str | None:
    cleaned = _normalize_text(value)
    if not cleaned:
        return None
    return cleaned.lower()


def _to_tier(confidence: float) -> str:
    if confidence >= 0.75:
        return "high"
    if confidence >= 0.45:
        return "medium"
    return "low"


def _is_login_or_authwall_url(value: str | None) -> bool:
    current = (value or "").lower()
    return "linkedin.com/login" in current or "linkedin.com/authwall" in current


def _extract_username_from_profile_url(profile_url: str) -> str:
    parts = urlsplit(profile_url)
    segments = [seg for seg in parts.path.split("/") if seg]
    if len(segments) >= 2 and segments[0].lower() == "in":
        return segments[1].lower()
    return "linkedin"


def _count_non_empty(values: dict[str, Any]) -> int:
    return sum(1 for value in values.values() if value not in (None, False, 0, "", [], {}, ()))


class LinkedInPPF:
    """Playwright-backed LinkedIn adapter with defensive scrape controls."""

    _counter_lock = asyncio.Lock()
    _delay_lock = asyncio.Lock()
    _cooldown_lock = asyncio.Lock()
    _last_scrape_started_at: float | None = None
    _rate_limit_cooldown_until: float | None = None

    def __init__(
        self,
        *,
        session_path: Path | None = None,
        counter_path: Path | None = None,
        daily_limit: int | None = None,
        min_delay: float | None = None,
        max_delay: float | None = None,
        scrape_timeout: float | None = None,
        headless: bool | None = None,
    ) -> None:
        self._session_path = (session_path or _DEFAULT_SESSION_PATH).expanduser()
        self._session_cookie = _session_cookie_from_env()
        if self._session_cookie is None:
            _validate_session_file(self._session_path)
        else:
            _validate_session_cookie(self._session_cookie)

        self._counter_path = (counter_path or _env_path("LINKEDIN_COUNTER_PATH", "./linkedin_daily_counter.json")).expanduser()
        self._daily_limit = max(1, int(daily_limit if daily_limit is not None else _env_int("LINKEDIN_DAILY_LIMIT", 40)))

        configured_min = min_delay if min_delay is not None else _env_float("LINKEDIN_MIN_DELAY", 3.0)
        configured_max = max_delay if max_delay is not None else _env_float("LINKEDIN_MAX_DELAY", 8.0)
        self._min_delay = max(0.0, float(configured_min))
        self._max_delay = max(self._min_delay, float(configured_max))

        self._scrape_timeout = max(
            0.01,
            float(scrape_timeout if scrape_timeout is not None else _env_float("LINKEDIN_SCRAPE_TIMEOUT", 30.0)),
        )
        self._headless = bool(headless if headless is not None else _env_bool("LINKEDIN_HEADLESS", True))
        self._retry_backoff_base = max(0.0, _env_float("LINKEDIN_RETRY_BACKOFF_BASE", 2.0))
        self._retry_backoff_max = max(self._retry_backoff_base, _env_float("LINKEDIN_RETRY_BACKOFF_MAX", 10.0))
        self._rate_limit_cooldown_seconds = max(0.0, _env_float("LINKEDIN_RATE_LIMIT_COOLDOWN", 900.0))

    async def run(self, subject_query: SubjectQuery) -> list[IdentityClaim]:
        """Run LinkedIn profile scrape for one SubjectQuery and return 0..1 claim."""
        normalized_url = normalize_linkedin_profile_url(getattr(subject_query, "linkedin_url", None))
        if not normalized_url:
            logger.warning("%s: invalid or absent linkedin_url, skipping", _SOURCE)
            return []

        # Re-read cookie fresh on every run so .env edits take effect
        # without restarting the server.
        cookie = _session_cookie_from_env()
        if cookie is not None:
            _validate_session_cookie(cookie)
            self._session_cookie = cookie
        else:
            _validate_session_file(self._session_path)
            self._session_cookie = self._read_cookie_from_session_file()

        cooldown_remaining = await self._cooldown_remaining_seconds()
        if cooldown_remaining > 0:
            message = (
                "LinkedIn rate-limit cooldown active. "
                f"Retry after approximately {int(cooldown_remaining)}s."
            )
            logger.warning("%s: %s", _SOURCE, message)
            return [
                self._build_url_anchor_claim(
                    subject_query=subject_query,
                    profile_url=normalized_url,
                    scrape_limited=True,
                    error_message=message,
                )
            ]

        max_attempts = 2
        last_exc: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                await self._reserve_daily_slot()
                claim = await asyncio.wait_for(
                    self._scrape_once(subject_query, normalized_url),
                    timeout=self._scrape_timeout,
                )
                return [claim] if claim is not None else []

            except LinkedInSessionExpiredError as exc:
                logger.critical("%s: session expired while scraping %s | %s", _SOURCE, normalized_url, exc)
                if exc.partial_claim is not None:
                    return [exc.partial_claim]
                raise

            except LinkedInSessionMissingError as exc:
                logger.critical("%s: session missing/invalid for %s | %s", _SOURCE, normalized_url, exc)
                raise

            except LinkedInRateLimitReachedError as exc:
                logger.warning("%s: %s", _SOURCE, exc)
                return []

            except asyncio.TimeoutError:
                logger.warning(
                    "%s: scrape timeout after %.1fs for %s (attempt %d/%d)",
                    _SOURCE,
                    self._scrape_timeout,
                    normalized_url,
                    attempt,
                    max_attempts,
                )
                last_exc = asyncio.TimeoutError()
                if attempt < max_attempts:
                    await asyncio.sleep(self._retry_backoff_seconds(attempt))
                    continue
                return []

            except Exception as exc:
                message = str(exc).lower()
                if "not logged in" in message or "authenticate before scraping" in message:
                    logger.error(
                        "%s: auth failed for %s (attempt %d/%d) | %s",
                        _SOURCE, normalized_url, attempt, max_attempts, exc,
                    )
                    last_exc = exc
                    if attempt < max_attempts:
                        # Re-read cookie in case it was updated between attempts
                        fresh = _session_cookie_from_env()
                        if fresh:
                            self._session_cookie = fresh
                        await asyncio.sleep(self._retry_backoff_seconds(attempt))
                        continue
                    raise LinkedInSessionExpiredError(
                        "LinkedIn authentication failed after retry. "
                        "Verify the li_at cookie is fresh and not expired on LinkedIn's side."
                    ) from exc

                if self._looks_rate_limited(message):
                    logger.warning("%s: rate limited for %s | %s", _SOURCE, normalized_url, exc)
                    await self._activate_rate_limit_cooldown()
                    return [
                        self._build_url_anchor_claim(
                            subject_query=subject_query,
                            profile_url=normalized_url,
                            scrape_limited=True,
                            error_message=str(exc),
                        )
                    ]

                logger.exception("%s: unexpected failure while scraping %s", _SOURCE, normalized_url)
                return []

        return []

    async def _scrape_once(
        self,
        subject_query: SubjectQuery,
        profile_url: str,
    ) -> IdentityClaim | None:
        await self._apply_inter_scrape_delay()

        cookie_value = self._resolve_cookie_value()
        logger.info(
            "%s: starting scrape for %s (cookie length=%d, headless=%s)",
            _SOURCE, profile_url, len(cookie_value), self._headless,
        )

        async with BrowserManager(headless=self._headless) as browser:
            # Inject the li_at cookie directly via the library's
            # login_with_cookie() which calls page.context.add_cookies().
            # This replaces the broken load_session() approach that expected
            # Playwright storage-state format but received {"li_at": "..."}.
            try:
                await login_with_cookie(browser.page, cookie_value)
            except Exception as exc:
                logger.error(
                    "%s: login_with_cookie failed for %s | %s",
                    _SOURCE, profile_url, exc,
                )
                raise LinkedInSessionExpiredError(
                    f"Cookie injection failed — the li_at cookie may be expired: {exc}",
                ) from exc

            scraper = PersonScraper(browser.page)
            person = await scraper.scrape(profile_url)

            current_url = _normalize_text(getattr(browser.page, "url", ""))
            if _is_login_or_authwall_url(current_url):
                partial_claim = None
                if _normalize_text(getattr(person, "name", None)):
                    partial_claim = self._build_claim(
                        subject_query=subject_query,
                        person=person,
                        profile_url=profile_url,
                        force_needs_review=True,
                        session_expired=True,
                    )
                raise LinkedInSessionExpiredError(
                    "LinkedIn redirected to login/authwall; session cookie likely expired",
                    partial_claim=partial_claim,
                )

            claim = self._build_claim(
                subject_query=subject_query,
                person=person,
                profile_url=profile_url,
                force_needs_review=False,
                session_expired=False,
            )
            await self._publish_audit(profile_url=profile_url, claim=claim)
            return claim

    @staticmethod
    def _looks_rate_limited(message: str) -> bool:
        keywords = (
            "rate limit",
            "rate-limited",
            "checkpoint",
            "captcha",
            "too many requests",
            "slow down",
            "try again later",
        )
        return any(keyword in message for keyword in keywords)

    def _build_url_anchor_claim(
        self,
        *,
        subject_query: SubjectQuery,
        profile_url: str,
        scrape_limited: bool,
        error_message: str | None = None,
    ) -> IdentityClaim:
        anchor_person = SimpleNamespace(
            name=None,
            headline=None,
            location=None,
            about=None,
            open_to_work=False,
            profile_picture_url=None,
            connections_count=None,
            follower_count=None,
            languages=[],
            contacts=[],
            experiences=[],
            educations=[],
        )

        claim = self._build_claim(
            subject_query=subject_query,
            person=anchor_person,
            profile_url=profile_url,
            force_needs_review=True,
            session_expired=False,
        )

        raw_data = dict(claim.raw_data or {})
        signal_groups = dict(raw_data.get("signal_groups") or {})
        signal_groups["tier_1_identity_anchors"] = {
            "canonical_url": profile_url,
            "vanity_name": claim.username,
            "full_name": None,
            "headline": None,
            "location": None,
            "email": claim.email,
        }
        signal_groups["tier_2_profile_enrichment"] = {
            "summary": None,
            "profile_picture_url": None,
            "connections_count": None,
            "follower_count": None,
            "is_open_to_work": False,
            "languages": [],
        }
        signal_groups["tier_5_confidence_signals"] = {
            "profile_completeness": 1,
            "has_profile_photo": False,
            "connection_degree": None,
            "is_verified": False,
            "activity_recency": None,
        }

        raw_data.update(
            {
                "scrape_limited": scrape_limited,
                "scrape_error": error_message,
                "signal_groups": signal_groups,
            }
        )
        claim.raw_data = raw_data
        claim.confidence = min(claim.confidence, 0.25)
        claim.tier = "low"
        claim.verified = False
        return claim

    def _retry_backoff_seconds(self, attempt: int) -> float:
        if self._retry_backoff_base <= 0:
            return 0.0
        seconds = self._retry_backoff_base * (2 ** max(0, attempt - 1))
        return min(self._retry_backoff_max, seconds)

    async def _activate_rate_limit_cooldown(self) -> None:
        if self._rate_limit_cooldown_seconds <= 0:
            return

        async with self._cooldown_lock:
            loop = asyncio.get_running_loop()
            new_until = loop.time() + self._rate_limit_cooldown_seconds
            existing = self.__class__._rate_limit_cooldown_until
            if existing is None or new_until > existing:
                self.__class__._rate_limit_cooldown_until = new_until

    async def _cooldown_remaining_seconds(self) -> float:
        async with self._cooldown_lock:
            until = self.__class__._rate_limit_cooldown_until
            if until is None:
                return 0.0
            now = asyncio.get_running_loop().time()
            if now >= until:
                self.__class__._rate_limit_cooldown_until = None
                return 0.0
            return until - now

    def _resolve_cookie_value(self) -> str:
        """Return the li_at cookie string from env override or session file."""
        if self._session_cookie:
            return self._session_cookie
        return self._read_cookie_from_session_file()

    def _read_cookie_from_session_file(self) -> str:
        """Read li_at value from the session JSON file."""
        try:
            raw = self._session_path.read_text(encoding="utf-8")
            payload = json.loads(raw)
            cookie = payload.get("li_at", "")
            if not cookie:
                raise LinkedInSessionMissingError(_session_invalid_message(self._session_path))
            return cookie
        except (json.JSONDecodeError, OSError) as exc:
            raise LinkedInSessionMissingError(_session_missing_message(self._session_path)) from exc

    async def _reserve_daily_slot(self) -> None:
        today = datetime.now(tz=timezone.utc).date().isoformat()

        async with self._counter_lock:
            state = await asyncio.to_thread(self._read_counter_state)
            state_date = str(state.get("date") or "")
            count_raw = state.get("count", 0)
            count = int(count_raw) if isinstance(count_raw, int | float) else 0

            if state_date != today:
                count = 0

            if count >= self._daily_limit:
                raise LinkedInRateLimitReachedError(
                    f"daily LinkedIn scrape limit reached ({count}/{self._daily_limit})"
                )

            count += 1
            await asyncio.to_thread(
                self._write_counter_state,
                {"date": today, "count": count},
            )

    def _read_counter_state(self) -> dict[str, Any]:
        if not self._counter_path.exists():
            return {}
        try:
            payload = json.loads(self._counter_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _write_counter_state(self, payload: dict[str, Any]) -> None:
        self._counter_path.parent.mkdir(parents=True, exist_ok=True)
        self._counter_path.write_text(
            json.dumps(payload, ensure_ascii=True),
            encoding="utf-8",
        )

    async def _apply_inter_scrape_delay(self) -> None:
        if self._max_delay <= 0:
            return

        async with self._delay_lock:
            loop = asyncio.get_running_loop()
            now = loop.time()

            if self._last_scrape_started_at is None:
                self.__class__._last_scrape_started_at = now
                return

            required_gap = random.uniform(self._min_delay, self._max_delay)
            elapsed = now - self._last_scrape_started_at
            sleep_for = max(0.0, required_gap - elapsed)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            self.__class__._last_scrape_started_at = loop.time()

    def _build_claim(
        self,
        *,
        subject_query: SubjectQuery,
        person: Any,
        profile_url: str,
        force_needs_review: bool,
        session_expired: bool,
    ) -> IdentityClaim:
        name = _normalize_text(getattr(person, "name", None))
        normalized_name = _normalize_name(name)
        headline = _normalize_text(getattr(person, "headline", None))
        location = _normalize_text(getattr(person, "location", None))
        about = _normalize_text(getattr(person, "about", None))
        open_to_work = bool(getattr(person, "open_to_work", False))

        profile_picture_url = _normalize_text(
            getattr(person, "profile_picture_url", None)
            or getattr(person, "picture_url", None)
            or getattr(person, "photo_url", None)
        )
        connections_count = self._normalize_optional_int(getattr(person, "connections_count", None))
        follower_count = self._normalize_optional_int(getattr(person, "follower_count", None))
        languages = self._serialize_languages(getattr(person, "languages", None))
        contacts = self._serialize_contacts(getattr(person, "contacts", None))

        experiences = self._serialize_experiences(getattr(person, "experiences", None))
        educations = self._serialize_educations(getattr(person, "educations", None))

        experience_count = len(experiences)
        education_count = len(educations)

        current_company = experiences[0].get("company_name") if experiences else None
        current_title = experiences[0].get("title") if experiences else None
        current_company_linkedin_url = experiences[0].get("company_linkedin_url") if experiences else None
        email = _normalize_text(getattr(subject_query, "email", None))

        vanity_name = _extract_username_from_profile_url(profile_url)
        profile_completeness = _count_non_empty(
            {
                "canonical_url": profile_url,
                "vanity_name": vanity_name,
                "full_name": name,
                "headline": headline,
                "location": location,
            "email": email,
                "summary": about,
                "profile_picture_url": profile_picture_url,
                "connections_count": connections_count,
                "follower_count": follower_count,
                "is_open_to_work": open_to_work,
                "languages": languages,
            }
        )

        no_exp_or_edu = experience_count == 0 and education_count == 0
        needs_review = force_needs_review or not normalized_name or no_exp_or_edu

        confidence = self._compute_confidence(
            has_name=bool(normalized_name),
            has_headline=bool(headline),
            experience_count=experience_count,
            education_count=education_count,
        )

        raw_data = {
            "name": name,
            "normalized_name": normalized_name,
            "headline": headline,
            "location": location,
            "about": about,
            "open_to_work": open_to_work,
            "profile_picture_url": profile_picture_url,
            "connections_count": connections_count,
            "follower_count": follower_count,
            "languages": languages,
            "contacts": contacts,
            "experiences": experiences,
            "educations": educations,
            "current_company": current_company,
            "current_title": current_title,
            "current_company_linkedin_url": current_company_linkedin_url,
            "experience_count": experience_count,
            "education_count": education_count,
            "profile_completeness": profile_completeness,
            "has_profile_photo": bool(profile_picture_url),
            "needs_review": needs_review,
            "session_expired": session_expired,
            "signal_groups": {
                "tier_1_identity_anchors": {
                    "canonical_url": profile_url,
                    "vanity_name": vanity_name,
                    "full_name": name,
                    "headline": headline,
                    "location": location,
                    "email": email,
                },
                "tier_2_profile_enrichment": {
                    "summary": about,
                    "profile_picture_url": profile_picture_url,
                    "connections_count": connections_count,
                    "follower_count": follower_count,
                    "is_open_to_work": open_to_work,
                    "languages": languages,
                },
                "tier_3_employment_history": experiences,
                "tier_4_education": educations,
                "tier_5_confidence_signals": {
                    "profile_completeness": profile_completeness,
                    "has_profile_photo": bool(profile_picture_url),
                    "connection_degree": None,
                    "is_verified": None,
                    "activity_recency": None,
                },
            },
        }

        institutions = [
            str(edu["institution"])
            for edu in educations
            if edu.get("institution")
        ]

        username = _extract_username_from_profile_url(profile_url)

        return IdentityClaim(
            platform=_PLATFORM,
            url=profile_url,
            username=username,
            email=email,
            confidence=confidence,
            tier=_to_tier(confidence),
            verified=not needs_review,
            source_tool=_SOURCE,
            institutions=institutions,
            raw_data=raw_data,
        )

    @staticmethod
    def _serialize_experiences(values: Any) -> list[dict[str, str | None]]:
        rows: list[dict[str, str | None]] = []
        for item in values or []:
            rows.append(
                {
                    "company_name": _normalize_text(
                        getattr(item, "institution_name", None)
                        or getattr(item, "company_name", None)
                        or getattr(item, "company", None)
                    ),
                    "company_linkedin_url": _normalize_text(getattr(item, "linkedin_url", None)),
                    "title": _normalize_text(
                        getattr(item, "position_title", None)
                        or getattr(item, "title", None)
                    ),
                    "location": _normalize_text(getattr(item, "location", None)),
                    "start_date": _normalize_text(
                        getattr(item, "from_date", None)
                        or getattr(item, "start_date", None)
                    ),
                    "end_date": _normalize_text(
                        getattr(item, "to_date", None)
                        or getattr(item, "end_date", None)
                    ),
                    "description": _normalize_text(getattr(item, "description", None)),
                }
            )
        return rows

    @staticmethod
    def _serialize_educations(values: Any) -> list[dict[str, str | None]]:
        rows: list[dict[str, str | None]] = []
        for item in values or []:
            rows.append(
                {
                    "school_name": _normalize_text(
                        getattr(item, "institution_name", None)
                        or getattr(item, "institution", None)
                        or getattr(item, "school_name", None)
                    ),
                    "school_linkedin_url": _normalize_text(getattr(item, "linkedin_url", None)),
                    "degree": _normalize_text(getattr(item, "degree", None)),
                    "field_of_study": _normalize_text(getattr(item, "field_of_study", None)),
                    "start_date": _normalize_text(
                        getattr(item, "from_date", None)
                        or getattr(item, "start_date", None)
                    ),
                    "end_date": _normalize_text(
                        getattr(item, "to_date", None)
                        or getattr(item, "end_date", None)
                    ),
                    "description": _normalize_text(getattr(item, "description", None)),
                }
            )
        return rows

    @staticmethod
    def _serialize_contacts(values: Any) -> list[dict[str, str | None]]:
        rows: list[dict[str, str | None]] = []
        for item in values or []:
            rows.append(
                {
                    "type": _normalize_text(getattr(item, "type", None)),
                    "value": _normalize_text(getattr(item, "value", None)),
                    "label": _normalize_text(getattr(item, "label", None)),
                }
            )
        return rows

    @staticmethod
    def _serialize_languages(values: Any) -> list[str]:
        languages: list[str] = []
        for item in values or []:
            name = _normalize_text(getattr(item, "name", None) or getattr(item, "language", None))
            if name:
                languages.append(name)
        return sorted(dict.fromkeys(languages))

    @staticmethod
    def _normalize_optional_int(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        match = re.search(r"\d[\d,]*", text)
        if not match:
            return None
        try:
            return int(match.group(0).replace(",", ""))
        except ValueError:
            return None

    @staticmethod
    def _compute_confidence(
        *,
        has_name: bool,
        has_headline: bool,
        experience_count: int,
        education_count: int,
    ) -> float:
        score = 0.6
        if has_name:
            score += 0.15
        if has_headline:
            score += 0.10
        if experience_count > 0:
            score += 0.05
        if education_count > 0:
            score += 0.05
        if experience_count == 0 and education_count == 0:
            score -= 0.25

        score = min(1.0, score)
        score = max(0.1, score)
        return round(score, 3)

    async def _publish_audit(self, *, profile_url: str, claim: IdentityClaim) -> None:
        payload = dict(claim.raw_data or {})

        envelope = {
            "subject_linkedin_url": profile_url,
            "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
            "source": _SOURCE,
            "payload": payload,
        }

        try:
            from maigret.kafka.producer import get_kafka_producer

            producer = get_kafka_producer()
            body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
            producer.publish(body, key=profile_url, topic=_KAFKA_TOPIC)
        except Exception as exc:
            logger.error(
                "%s: Kafka audit publish failed for %s | %s",
                _SOURCE,
                profile_url,
                exc,
            )


__all__ = [
    "LinkedInPPF",
    "LinkedInSessionMissingError",
    "LinkedInSessionExpiredError",
    "LinkedInRateLimitReachedError",
    "SubjectQuery",
    "normalize_linkedin_profile_url",
]
