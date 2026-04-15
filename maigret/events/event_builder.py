"""
events/event_builder.py — Construct canonical ``osint.raw.maigret.v1`` Kafka events.

Production rewrite: uses Pydantic ``MaigretEvent`` model for schema validation.
"""

from __future__ import annotations

import logging
from typing import Any

from maigret.models import (
    EventProfile,
    MaigretEvent,
    PipelineStats,
    QueryMeta,
    ScoredProfile,
)

logger = logging.getLogger(__name__)


def _profile_to_event_profile(profile: ScoredProfile | dict | Any) -> EventProfile:
    """Normalise a PPF profile (model or dict) into the canonical event shape."""
    if isinstance(profile, ScoredProfile):
        return EventProfile(
            platform=profile.platform,
            url=profile.url,
            username=profile.username,
            confidence=profile.confidence,
            verified=profile.http_status == 200,
            tags=[],
        )

    if isinstance(profile, dict):
        d = profile
    elif hasattr(profile, "__dict__") and not isinstance(profile, str):
        try:
            d = vars(profile)
        except TypeError:
            raise ValueError(f"Cannot convert {type(profile).__name__} to EventProfile")
    else:
        raise ValueError(f"Unsupported profile type: {type(profile).__name__}")

    return EventProfile(
        platform=str(d.get("platform") or "unknown").lower(),
        url=d.get("url") or d.get("url_user"),
        username=d.get("username"),
        confidence=_safe_float(d.get("confidence", d.get("score", 0.0))),
        verified=bool(d.get("verified", d.get("http_verified", False))),
        tags=list(d.get("tags") or []),
    )


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def build_event(
    username: str,
    profiles: list[ScoredProfile | dict],
    total_raw: int,
    total_claimed: int,
    trigger: str = "api",
) -> tuple[bytes, str]:
    """
    Build a serialised Kafka event for ``osint.raw.maigret.v1``.

    Returns:
        ``(payload_bytes, partition_key)``
    """
    normalised: list[EventProfile] = []
    skipped = 0

    for p in profiles:
        try:
            normalised.append(_profile_to_event_profile(p))
        except Exception as exc:
            skipped += 1
            logger.warning("Skipping malformed profile: %s | error: %s", p, exc)

    if skipped:
        logger.warning(
            "build_event: skipped %d malformed profiles for user=%s",
            skipped, username,
        )

    event = MaigretEvent(
        query=QueryMeta(username=username, trigger=trigger),
        stats=PipelineStats(
            total_raw=total_raw,
            total_claimed=total_claimed,
            total_filtered=len(normalised),
            skipped_malformed=skipped,
        ),
        profiles=normalised,
    )

    payload_bytes = event.model_dump_json().encode("utf-8")
    partition_key = username

    logger.info(
        "Built event id=%s user=%s profiles=%d bytes=%d",
        event.event_id, username, len(normalised), len(payload_bytes),
    )

    return payload_bytes, partition_key