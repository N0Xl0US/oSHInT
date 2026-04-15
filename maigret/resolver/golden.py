"""
resolver/golden.py — Stage 4: Golden Record merge from resolved entity clusters.

Implements conflict-aware merge logic:
    - Multi-source corroboration boost (claims from multiple independent tools)
  - Recency selection for conflicting values (highest-confidence proxy)
  - Merge history tracking with per-fragment scores
  - Quality flag calculation from handover.md §1 thresholds
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Sequence

from maigret.agent.report import (
    Alias,
    GoldenRecord,
    GoldenRecordAttributes,
    IdentityClaim,
    MergeRecord,
    QualityFlags,
    SourceRecord,
)
from maigret.resolver.config import ResolverConfig, get_resolver_config
from maigret.resolver.flatten import _make_unique_id
from maigret.resolver.linker import ResolvedCluster

logger = logging.getLogger(__name__)


# Tier weights for confidence averaging
_TIER_WEIGHT: dict[str, float] = {
    "high":   2.0,
    "medium": 1.0,
    "low":    0.5,
    "drop":   0.0,
}

# Multi-source corroboration multiplier
# If a claim is corroborated by multiple tools, boost confidence
_CORROBORATION_BOOST = 1.3


def build_golden_records(
    clusters: Sequence[ResolvedCluster],
    config: ResolverConfig | None = None,
) -> list[GoldenRecord]:
    """Convert resolved clusters into GoldenRecord objects."""
    cfg = config or get_resolver_config()
    records: list[GoldenRecord] = []

    for cluster in clusters:
        if not cluster.claims:
            continue
        record = _cluster_to_golden_record(cluster, cfg)
        records.append(record)

    logger.info("Built %d golden records from %d clusters", len(records), len(clusters))
    return records


def _cluster_to_golden_record(
    cluster: ResolvedCluster,
    config: ResolverConfig,
) -> GoldenRecord:
    """Assemble a single GoldenRecord with conflict-aware merge."""
    claims = cluster.claims

    # ── Detect multi-source corroboration ─────────────────────────────────
    source_tools = set(c.source_tool for c in claims)
    is_corroborated = len(source_tools) > 1

    # ── Aliases (deduplicated, conflict-resolved) ─────────────────────────
    aliases = _build_aliases(claims, is_corroborated, config.pii_hmac_secret)

    # ── Confidence scoring (tier-weighted + corroboration boost) ──────────
    overall_confidence = _compute_confidence(claims, is_corroborated)

    # ── Conflict detection ────────────────────────────────────────────────
    conflicts = _detect_conflicts(claims)

    # ── Quality flags (handover.md §1 thresholds) ─────────────────────────
    quality_flags = QualityFlags(
        low_confidence=overall_confidence < config.confidence_flag_ingest,
        conflicting_data=len(conflicts) > 0,
        quarantined=overall_confidence < config.confidence_quarantine,
        needs_review=(
            overall_confidence < config.confidence_direct_ingest
            or len(conflicts) > 0
        ),
    )

    # ── Source records ────────────────────────────────────────────────────
    sources = [
        SourceRecord(
            tool=tool,
            reliability_weight=0.8 if tool == "maigret" else 0.6,
        )
        for tool in sorted(source_tools)
    ]

    # ── Merge history ─────────────────────────────────────────────────────
    merge_history = [
        MergeRecord(
            fragment_id=_make_unique_id(claim),
            merge_score=claim.confidence,
        )
        for claim in claims
    ]

    # ── Attributes ────────────────────────────────────────────────────────
    name_variants = sorted(set(_pseudonymize(c.username, config.pii_hmac_secret) for c in claims))
    institutions = sorted({inst for c in claims for inst in c.institutions if inst})
    research_domains = sorted({domain for c in claims for domain in c.research_domains if domain})
    co_authors = sorted({author for c in claims for author in c.co_authors if author})

    return GoldenRecord(
        confidence_score=overall_confidence,
        resolution_method=cluster.resolution_method,
        aliases=aliases,
        attributes=GoldenRecordAttributes(
            name_variants=name_variants,
            institutions=institutions,
            research_domains=research_domains,
            co_authors=co_authors,
        ),
        sources=sources,
        merge_history=merge_history,
        quality_flags=quality_flags,
    )


def _build_aliases(
    claims: list[IdentityClaim],
    is_corroborated: bool,
    hmac_secret: str,
) -> list[Alias]:
    """
    Build deduplicated aliases with conflict resolution.

    When same platform has conflicting usernames → pick highest confidence
    (proxy for most authoritative source). Lower-confidence variant still
    appears as an alias but with reduced confidence.
    """
    # Group by (type, platform)
    platform_claims: dict[tuple[str, str | None], list[IdentityClaim]] = {}
    for claim in claims:
        key = ("username", claim.platform)
        platform_claims.setdefault(key, []).append(claim)

    aliases: list[Alias] = []
    seen: set[tuple[str, str]] = set()

    for (alias_type, platform), group in platform_claims.items():
        # Sort by confidence descending — first wins on conflict
        sorted_group = sorted(group, key=lambda c: c.confidence, reverse=True)

        for i, claim in enumerate(sorted_group):
            dedup_key = (claim.username.lower(), claim.platform)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            conf = claim.confidence
            # boost if corroborated by multiple tools
            if is_corroborated and i == 0:
                conf = min(conf * _CORROBORATION_BOOST, 1.0)

            aliases.append(Alias(
                type=alias_type,
                value=_pseudonymize(claim.username, hmac_secret),
                platform=claim.platform,
                confidence=round(conf, 4),
                source=claim.source_tool,
            ))

    # Add email aliases
    seen_emails: set[str] = set()
    for claim in claims:
        if claim.email and claim.email.lower() not in seen_emails:
            seen_emails.add(claim.email.lower())
            aliases.append(Alias(
                type="email",
                value=_pseudonymize(claim.email, hmac_secret),
                platform=None,
                confidence=round(claim.confidence * (
                    _CORROBORATION_BOOST if is_corroborated else 1.0
                ), 4),
                source=claim.source_tool,
            ))

    # Add phone aliases
    seen_phones: set[str] = set()
    for claim in claims:
        if claim.phone and claim.phone not in seen_phones:
            seen_phones.add(claim.phone)
            aliases.append(Alias(
                type="phone",
                value=_pseudonymize(claim.phone, hmac_secret),
                platform=None,
                confidence=round(claim.confidence, 4),
                source=claim.source_tool,
            ))

    return aliases


def _compute_confidence(claims: list[IdentityClaim], is_corroborated: bool) -> float:
    """
    Tier-weighted confidence score with corroboration boost.

    Weighting:
      high-tier claims:   2.0x weight
      medium-tier claims: 1.0x weight
      low-tier claims:    0.5x weight
      verified claims:    1.25x bonus
      corroborated:       1.3x boost on final score
    """
    total_weight = 0.0
    weighted_sum = 0.0

    for claim in claims:
        w = _TIER_WEIGHT.get(claim.tier, 1.0)
        if claim.verified:
            w *= 1.25
        weighted_sum += claim.confidence * w
        total_weight += w

    if total_weight == 0:
        return 0.0

    base_score = weighted_sum / total_weight

    # Multi-source corroboration boost
    if is_corroborated:
        base_score = min(base_score * _CORROBORATION_BOOST, 1.0)

    return round(min(max(base_score, 0.0), 1.0), 4)


def _detect_conflicts(claims: list[IdentityClaim]) -> list[dict]:
    """
    Detect conflicting data within a cluster.

    Conflict types:
      1. Same platform, different usernames (might be different people)
      2. Same email claimed by different usernames
    """
    conflicts: list[dict] = []

    # Type 1: same platform, different usernames
    platform_usernames: dict[str, set[str]] = {}
    for claim in claims:
        platform_usernames.setdefault(claim.platform, set()).add(claim.username.lower())

    for platform, usernames in platform_usernames.items():
        if len(usernames) > 1:
            conflicts.append({
                "type": "platform_username_conflict",
                "platform": platform,
                "values": sorted(usernames),
                "resolution": "highest_confidence_wins",
            })

    # Type 2: same email, different usernames
    email_usernames: dict[str, set[str]] = {}
    for claim in claims:
        if claim.email:
            email_usernames.setdefault(claim.email.lower(), set()).add(claim.username.lower())

    for email, usernames in email_usernames.items():
        if len(usernames) > 1:
            conflicts.append({
                "type": "email_username_conflict",
                "email_prefix": email.split("@")[0][:3] + "***",
                "values": sorted(usernames),
                "resolution": "both_retained_as_aliases",
            })

    if conflicts:
        logger.warning("Detected %d conflicts in cluster", len(conflicts))

    return conflicts


def _pseudonymize(value: str | None, secret: str) -> str:
    """HMAC-SHA256 pseudonymization for PII-bearing fields."""
    raw = (value or "").strip().lower()
    return hmac.new(secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()
