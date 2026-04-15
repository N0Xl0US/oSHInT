"""
ppf.py — Post-Processing Filter for maigret output.

Pipeline: raw profiles → dedup → classify → async HEAD verify → score → filter.

Production rewrite: uses Pydantic models, structured logging, extracted tier
constants, and proper error handling.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from urllib.parse import urlparse

from maigret.models import RawProfile, ScoredProfile
from maigret.tiers import (
    ADULT_DATING_PLATFORMS,
    API_URL_PATTERNS,
    HIGH_COLLISION_MEDIUM_PLATFORMS,
    HTTP_SCORE,
    MULTI_REGION_PLATFORMS,
    MULTI_URL_PLATFORMS,
    SEARCH_PAGE_PATTERNS,
    STRICT_URL_BLACKLIST,
    TIER_DROP,
    TIER_HIGH,
    TIER_LOW,
    TIER_MEDIUM,
    TIER_SCORE,
    TRUSTED_MEDIUM_PLATFORMS,
    WORKSPACE_URL_PATTERNS,
)

logger = logging.getLogger(__name__)


# ── Exceptions ──────────────────────────────────────────────────────────────


class PPFError(Exception):
    """Base exception for PPF pipeline failures."""


# ── URL classifiers ─────────────────────────────────────────────────────────


def is_api_endpoint(url: str) -> bool:
    """True if the URL looks like a JSON API call rather than a profile page."""
    if not url:
        return False
    return any(pat in url for pat in API_URL_PATTERNS)


def is_workspace_url(url: str) -> bool:
    """True if the URL is a workspace/org (e.g. ``elonmusk.slack.com``)."""
    if not url:
        return False
    return any(pat in url for pat in WORKSPACE_URL_PATTERNS)


def is_search_page(url: str) -> bool:
    """True if the URL is a search/filter page, not a canonical profile."""
    if not url:
        return False
    return any(pat in url for pat in SEARCH_PAGE_PATTERNS)


def is_valid_profile(url: str) -> bool:
    """Strict URL gate: reject search, API, and template-generated URLs."""
    if not url:
        return False
    lowered = url.lower()
    return not any(bad in lowered for bad in STRICT_URL_BLACKLIST)


def is_trusted_medium(platform: str) -> bool:
    p = platform.lower().strip()
    return any(token in p for token in TRUSTED_MEDIUM_PLATFORMS)


def is_high_collision_medium(platform: str) -> bool:
    p = platform.lower().strip()
    return any(token in p for token in HIGH_COLLISION_MEDIUM_PLATFORMS)


# ── Platform tier lookup ────────────────────────────────────────────────────


def get_tier(platform: str) -> str:
    """Determine the signal tier for a platform name."""
    p = platform.lower().strip()

    for adult in ADULT_DATING_PLATFORMS:
        if adult in p:
            return "drop"

    for drop in TIER_DROP:
        if drop in p:
            return "drop"

    for low in TIER_LOW:
        if low in p:
            return "low"

    for high in TIER_HIGH:
        if high in p:
            return "high"

    for medium in TIER_MEDIUM:
        if medium in p:
            return "medium"

    return "medium"  # unknown → medium, not dropped


# ── Deduplicator ─────────────────────────────────────────────────────────────


def deduplicate(profiles: list[dict]) -> list[dict]:
    """
    Collapse duplicates:
    - Exact duplicate URLs
    - Multi-region platforms (op.gg) → single entry with regions list
    - Multi-URL platforms (steam /id/ + /groups/, github + githubgist) → canonical
    """
    seen_urls: set[str] = set()
    seen_buckets: set[str] = set()
    region_buckets: dict[str, list[str]] = {}
    unique: list[dict] = []

    for p in profiles:
        url = (p.get("url") or "").strip()
        platform = (p.get("platform") or "").lower()

        # Multi-region collapse (op.gg)
        if any(mr in platform for mr in MULTI_REGION_PLATFORMS):
            parsed = urlparse(url)
            region = ""
            for part in parsed.query.split("&"):
                if part.startswith("region="):
                    region = part.split("=", 1)[1]
            bucket_key = "op.gg"
            region_buckets.setdefault(bucket_key, [])
            if region and region not in region_buckets[bucket_key]:
                region_buckets[bucket_key].append(region)
            if bucket_key not in seen_urls:
                seen_urls.add(bucket_key)
                unique.append({**p, "_opgg_bucket": bucket_key})
            continue

        # Multi-URL platform collapse (steam, github/githubgist)
        canonical = MULTI_URL_PLATFORMS.get(platform)
        if canonical:
            if canonical not in seen_buckets:
                seen_buckets.add(canonical)
                unique.append({**p, "platform": canonical})
            continue

        # Standard dedup by URL
        if url in seen_urls:
            continue
        seen_urls.add(url)
        unique.append(p)

    # Attach regions to op.gg entries
    for p in unique:
        bucket_key = p.pop("_opgg_bucket", None)
        if bucket_key:
            p["_regions"] = region_buckets.get(bucket_key, [])

    return unique


# ── Async HTTP HEAD verifier ─────────────────────────────────────────────────

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; OSINT-PPF/2.0)"}


async def _head_check_single(
    url: str,
    session: "aiohttp.ClientSession",
    semaphore: asyncio.Semaphore,
    timeout: int = 5,
) -> int:
    """Async HEAD request. Returns status code, or 0 on failure."""
    if not url or not url.startswith("http"):
        return 0
    async with semaphore:
        try:
            async with session.head(
                url,
                headers=_HEADERS,
                timeout=timeout,
                allow_redirects=False,
                ssl=False,
            ) as resp:
                return resp.status
        except Exception:
            return 0


async def head_check_all(
    urls: list[str],
    timeout: int = 5,
    max_concurrency: int = 20,
) -> dict[str, int]:
    """
    Fire concurrent HEAD requests for all URLs, capped by semaphore.

    Returns ``{url: status_code}``.
    Falls back to serial stdlib requests if aiohttp is unavailable.
    """
    try:
        import aiohttp
    except ImportError:
        logger.warning(
            "aiohttp not installed — falling back to serial HEAD checks. "
            "Install with: pip install aiohttp"
        )
        return _head_check_serial_fallback(urls, timeout)

    semaphore = asyncio.Semaphore(max_concurrency)
    connector = aiohttp.TCPConnector(limit=max_concurrency, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = {
            url: asyncio.create_task(
                _head_check_single(url, session, semaphore, timeout)
            )
            for url in urls
            if url
        }
        gathered = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            url: (res if isinstance(res, int) else 0)
            for url, res in zip(tasks.keys(), gathered)
        }


def _head_check_serial_fallback(urls: list[str], timeout: int) -> dict[str, int]:
    """Pure-stdlib fallback when aiohttp is not available."""
    import urllib.error
    import urllib.request

    results: dict[str, int] = {}
    for url in urls:
        if not url or not url.startswith("http"):
            results[url] = 0
            continue
        try:
            req = urllib.request.Request(url, method="HEAD")
            req.add_header("User-Agent", _HEADERS["User-Agent"])
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                results[url] = resp.status
        except urllib.error.HTTPError as e:
            results[url] = e.code
        except Exception:
            results[url] = 0
    return results


# ── Scorer ───────────────────────────────────────────────────────────────────


def score_profile(tier: str, http_status: int, is_api: bool) -> float:
    """Compute confidence score from tier weight + HTTP response weight."""
    if is_api or tier == "drop":
        return 0.0
    tier_weight = TIER_SCORE.get(tier, 0.2)
    http_weight = HTTP_SCORE.get(http_status, 0.0)
    return round(tier_weight + http_weight, 2)


def _signal_bonus(platform: str, tier: str, http_status: int) -> float:
    """Add signal-specific granularity to reduce flat tier-only confidence bands."""
    # When we already have a hard 200 verification, avoid extra optimistic boosts.
    if http_status == 200:
        return 0.0

    p = (platform or "").lower().strip()

    if tier == "high":
        if p in {"github", "gitlab", "linkedin", "kaggle", "hackernews", "youtube"}:
            return 0.08
        return 0.03

    if tier == "medium":
        if "researchgate" in p or "academia" in p:
            return 0.06
        if p in {"twitter", "reddit", "telegram", "dribbble", "behance", "codepen", "hashnode", "medium"}:
            return 0.04
        return 0.02

    if tier == "low":
        return 0.01

    return 0.0


# ── Pipeline entry point ────────────────────────────────────────────────────


async def run_ppf(
    raw_profiles: list[RawProfile],
    *,
    verify_http: bool = True,
    http_timeout: int = 5,
    max_concurrency: int = 20,
    min_confidence: float = 0.3,
) -> list[ScoredProfile]:
    """
    Async PPF pipeline:
      raw profiles → dedup → classify → concurrent HEAD verify → score → filter.

    Args:
        raw_profiles:    List of ``RawProfile`` models from maigret.
        verify_http:     Whether to HEAD-verify surviving URLs.
        http_timeout:    Per-request HEAD timeout in seconds.
        max_concurrency: Concurrent HEAD requests cap.
        min_confidence:  Minimum confidence score to pass filtering.

    Returns:
        Sorted list of ``ScoredProfile`` models (highest confidence first).
    """
    logger.info("PPF input: %d raw profiles", len(raw_profiles))

    # Stage 1: Deduplication (operates on dicts, then back to models)
    raw_dicts = [p.model_dump() if hasattr(p, "model_dump") else p for p in raw_profiles]
    deduped = deduplicate(raw_dicts)
    logger.info(
        "PPF dedup: %d → %d (%d removed)",
        len(raw_profiles), len(deduped), len(raw_profiles) - len(deduped),
    )

    # Stage 2–3: Classify
    classified: list[dict] = []
    for p in deduped:
        platform = (p.get("platform") or "").lower().strip()
        url = (p.get("url") or "").strip()
        username = (p.get("username") or "").lower().strip()
        check_type = (p.get("check_type") or "unknown").strip().lower()
        try:
            source_http_status = int(p.get("source_http_status") or 0)
        except (TypeError, ValueError):
            source_http_status = 0
        has_ids = bool(p.get("has_ids"))
        regions = p.get("_regions", [])
        lowered_url = url.lower()

        # Mandatory identity gate: URL must contain the queried username
        if not username or username not in lowered_url:
            classified.append({
                "platform": platform, "url": url, "username": username,
                "tier": "drop", "is_api": False, "regions": regions,
                "drop_reason": "username_not_in_url",
            })
            logger.debug("DROP (username not in url): %s → %s", platform, url[:55])
            continue

        tier = get_tier(platform)
        api = is_api_endpoint(url)
        valid = is_valid_profile(url)

        # Workspace URLs → drop-equivalent
        if not api and is_workspace_url(url):
            api = True

        # Search/filter/generated URLs → hard drop
        if not api and (not valid or is_search_page(url)):
            tier = "drop"

        drop_reason: str | None = None
        if api:
            drop_reason = "workspace_or_api"
        elif not valid:
            drop_reason = "invalid_profile_url"
        elif is_search_page(url):
            drop_reason = "search_or_generated_url"
        elif any(tag in platform for tag in ADULT_DATING_PLATFORMS):
            drop_reason = "adult_or_dating_platform"
        elif tier == "medium" and is_high_collision_medium(platform):
            drop_reason = "high_collision_medium_platform"
            tier = "drop"
        elif tier == "medium" and not is_trusted_medium(platform):
            drop_reason = "untrusted_medium_platform"
            tier = "drop"
        elif check_type == "status_code" and source_http_status == 200 and not has_ids:
            # Many sites return 200 for non-existent profiles. If Maigret has no
            # structured IDs and relied only on status code, treat as weak claim.
            drop_reason = "status_code_only_claim_without_ids"
            tier = "drop"
        elif tier == "drop":
            drop_reason = "low_signal_platform"

        if drop_reason:
            logger.debug("DROP (%s): %s → %s", drop_reason, platform, url[:55])

        classified.append({
            "platform": platform, "url": url, "username": username,
            "tier": tier, "is_api": api, "regions": regions,
            "drop_reason": drop_reason,
        })

    # Stage 4: Concurrent HEAD verification
    to_verify = [c["url"] for c in classified if not c["drop_reason"] and c["url"]]

    http_results: dict[str, int] = {}
    if verify_http and to_verify:
        logger.info(
            "HEAD verifying %d URLs (concurrency=%d, timeout=%ds)",
            len(to_verify), max_concurrency, http_timeout,
        )
        loop = asyncio.get_event_loop()
        t0 = loop.time()
        http_results = await head_check_all(to_verify, timeout=http_timeout, max_concurrency=max_concurrency)
        elapsed = loop.time() - t0
        ok = sum(1 for s in http_results.values() if s == 200)
        logger.info("HEAD complete in %.1fs — %d/%d returned 200", elapsed, ok, len(to_verify))
    elif not verify_http:
        logger.debug("HTTP verification skipped")

    # Stage 5: Score
    results: list[ScoredProfile] = []
    for c in classified:
        http_status = http_results.get(c["url"], 0)
        confidence = score_profile(c["tier"], http_status, c["is_api"])
        if not c["is_api"] and c["tier"] != "drop":
            confidence = round(min(1.0, confidence + _signal_bonus(c["platform"], c["tier"], http_status)), 2)

        results.append(ScoredProfile(
            platform=c["platform"],
            url=c["url"] or None,
            username=c["username"],
            tier="drop" if c["drop_reason"] else c["tier"],
            http_status=http_status,
            confidence=confidence,
            is_api_endpoint=c["is_api"],
            regions=c["regions"],
            drop_reason=c["drop_reason"],
        ))

    # Stage 6: Filter
    passed = [r for r in results if r.confidence >= min_confidence]
    dropped_count = len(results) - len(passed)

    tier_breakdown = ", ".join(
        f"{t}={sum(1 for r in passed if r.tier == t)}"
        for t in ("high", "medium", "low")
    )
    logger.info(
        "PPF complete: %d pass (≥%.2f), %d filtered | %s",
        len(passed), min_confidence, dropped_count, tier_breakdown,
    )

    return sorted(passed, key=lambda r: r.confidence, reverse=True)