"""
academic/ppf.py — Post-Processing Filter for academic search results.

Scores and merges author profiles from Semantic Scholar and OpenAlex
into confidence-ranked results using name matching, institutional
affiliation, ORCID anchoring, and cross-source corroboration.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from maigret.academic.models import AcademicAuthorProfile

logger = logging.getLogger(__name__)


# ── Name matching ────────────────────────────────────────────────────────────


def _normalise_name(name: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", name.strip().lower())


def _name_tokens(name: str) -> list[str]:
    """Tokenize a name into lowercase alphanumeric parts."""
    normalised = _normalise_name(name)
    return [tok for tok in re.split(r"[^a-z0-9]+", normalised) if tok]


def _passes_full_name_watch(query_name: str, author_name: str) -> bool:
    """Require first + last token match when query has a full name."""
    q_tokens = _name_tokens(query_name)
    a_tokens = _name_tokens(author_name)

    if not q_tokens or not a_tokens:
        return False

    # For single-token queries, fall back to exact token equality.
    if len(q_tokens) == 1:
        return len(a_tokens) == 1 and q_tokens[0] == a_tokens[0]

    if len(a_tokens) < 2:
        return False

    return q_tokens[0] == a_tokens[0] and q_tokens[-1] == a_tokens[-1]


def _name_match_score(query_name: str, author_name: str) -> float:
    """
    Compute a name similarity score between query and returned author.

    Returns 0.0–1.0.
    """
    q = _normalise_name(query_name)
    a = _normalise_name(author_name)

    if not q or not a:
        return 0.0

    # Exact match
    if q == a:
        return 1.0

    # One is a substring of the other (e.g. "John Smith" in "John D. Smith")
    if q in a or a in q:
        return 0.7

    # Token overlap: compare individual name parts
    q_tokens = set(q.split())
    a_tokens = set(a.split())
    if not q_tokens or not a_tokens:
        return 0.0

    overlap = len(q_tokens & a_tokens)
    total = max(len(q_tokens), len(a_tokens))
    token_score = overlap / total if total > 0 else 0.0

    return round(min(token_score * 0.9, 1.0), 4)


def _institution_boost(
    query_institution: str | None,
    affiliations: list[str],
) -> float:
    """Return +0.15 if the query institution matches any affiliation."""
    if not query_institution or not affiliations:
        return 0.0
    q = _normalise_name(query_institution)
    for aff in affiliations:
        if q in _normalise_name(aff):
            return 0.15
    return 0.0


def _publication_volume_weight(paper_count: int) -> float:
    """Small boost for prolific authors: min(paper_count / 50, 0.1)."""
    return round(min(paper_count / 50.0, 0.1), 4)


# ── Cross-source ORCID matching ─────────────────────────────────────────────


def _find_orcid_matches(
    profiles: list[AcademicAuthorProfile],
) -> dict[str, list[int]]:
    """Group profile indices by ORCID. Only includes ORCIDs seen ≥2 times."""
    orcid_map: dict[str, list[int]] = {}
    for i, p in enumerate(profiles):
        if p.orcid:
            orcid_map.setdefault(p.orcid, []).append(i)
    return {k: v for k, v in orcid_map.items() if len(v) >= 2}


# ── Main PPF pipeline ───────────────────────────────────────────────────────


def score_and_merge(
    profiles: list[AcademicAuthorProfile],
    query_name: str,
    query_institution: str | None = None,
    min_confidence: float = 0.3,
) -> list[AcademicAuthorProfile]:
    """
    Full-name watch, then score, boost, deduplicate, and filter profiles.

    Scoring factors:
    1. Full-name watch (must match first + last token when query has full name)
    2. Name-match score (0.0–1.0) — base signal
    3. Institution boost (+0.15 if affiliation matches)
    4. Publication volume weight (+0.0–0.1)
    5. ORCID anchor: profiles sharing an ORCID across sources → max(0.9, existing)
    6. Cross-source corroboration: +0.1 if same normalised name appears from
         different sources

    Returns profiles sorted by confidence desc, filtered by min_confidence.
    """
    if not profiles:
        return []

    logger.info(
        "Academic PPF: scoring %d profiles for query='%s' institution='%s'",
        len(profiles), query_name, query_institution,
    )

    # ── Step 0: Full-name watch ──────────────────────────────────────────
    watched = [
        p for p in profiles
        if _passes_full_name_watch(query_name, p.name)
    ]

    if not watched:
        logger.info("Academic PPF: 0 profiles after full-name watch")
        return []

    # ── Step 1: Base scoring ─────────────────────────────────────────────
    for p in watched:
        name_score = _name_match_score(query_name, p.name)
        inst_boost = _institution_boost(query_institution, p.affiliations)
        vol_weight = _publication_volume_weight(p.paper_count)
        p.confidence = round(min(name_score + inst_boost + vol_weight, 1.0), 4)

    # ── Step 2: Cross-source corroboration ───────────────────────────────
    name_source_map: dict[str, set[str]] = {}
    for p in watched:
        norm = _normalise_name(p.name)
        name_source_map.setdefault(norm, set()).add(p.source)

    corroborated_names = {
        name for name, sources in name_source_map.items() if len(sources) >= 2
    }
    for p in watched:
        if _normalise_name(p.name) in corroborated_names:
            p.confidence = round(min(p.confidence + 0.1, 1.0), 4)

    # ── Step 3: ORCID anchor ─────────────────────────────────────────────
    orcid_groups = _find_orcid_matches(watched)
    for _orcid, indices in orcid_groups.items():
        for i in indices:
            watched[i].confidence = round(max(0.9, watched[i].confidence), 4)

    # ── Step 4: Filter and sort ──────────────────────────────────────────
    filtered = [p for p in watched if p.confidence >= min_confidence]
    filtered.sort(key=lambda p: -p.confidence)

    logger.info(
        "Academic PPF: %d profiles after filtering (from %d input, %d full-name matches)",
        len(filtered), len(profiles), len(watched),
    )
    return filtered
