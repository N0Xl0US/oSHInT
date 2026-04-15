"""
resolver/flatten.py — Convert IdentityClaims to a pandas DataFrame for Splink.

Splink operates on tabular data. This module normalises claim objects into
a flat DataFrame with columns suitable for blocking and comparison.

Blocking columns are designed per the production spec:
  - email_hash:         SHA256 of lowered email (strongest blocking signal)
  - username_norm:      lowered + stripped username (exact blocking)
  - username_prefix:    first 4 chars of username_norm (coarse blocking)
  - platform_username:  compound key for cross-source blocking
  - url_domain:         extracted domain (domain-level comparison)
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
from typing import Sequence
from urllib.parse import urlparse

import pandas as pd

from maigret.agent.report import IdentityClaim


_EXPECTED_COLUMNS: list[str] = [
    "unique_id",
    "username",
    "username_norm",
    "username_prefix",
    "platform",
    "platform_prefix",
    "platform_username",
    "url_domain",
    "url_path_slug",
    "email_hash",
    "source_tool",
    "confidence",
    "tier",
    "verified",
    "url",
    "email",
    "phone",
]

_REQUIRED_CONTRACT_COLUMNS: set[str] = {
    "unique_id",
    "username_norm",
    "email_hash",
    "platform",
    "platform_prefix",
    "confidence",
    "source_tool",
    "verified",
}


def flatten_claims(claims: Sequence[IdentityClaim]) -> pd.DataFrame:
    """
    Convert a list of IdentityClaims into a Splink-ready DataFrame.

    Columns:
        unique_id          — deterministic hash (platform + url + username + source)
        username           — raw username from claim
        username_norm      — lowercase, special chars stripped (for fuzzy matching)
        username_prefix    — first 4 chars of username_norm (coarse blocking)
        platform           — lowercase platform name
        platform_username  — "{platform}:{username_norm}" compound key
        url_domain         — domain extracted from URL (or empty)
        url_path_slug      — first path segment from URL
        email_hash         — SHA256 of lowered email (or empty)
        source_tool        — e.g. 'maigret' | 'academic' | 'github'
        confidence         — float 0..1
        tier               — 'high' | 'medium' | 'low' | 'drop'
        verified           — bool
        url                — original URL (carried through, not used by Splink)
        email              — original email (carried through)
        phone              — original phone (carried through)
    """
    rows: list[dict] = []

    for claim in claims:
        uid = _make_unique_id(claim)
        unorm = _normalise_username(claim.username)
        platform = (claim.platform or "unknown").lower()
        e_hash = claim.email_hash or _hash_email(claim.email) if claim.email else ""

        rows.append({
            "unique_id":          uid,
            "username":           claim.username,
            "username_norm":      unorm,
            "username_prefix":    unorm[:4] if len(unorm) >= 4 else unorm,
            "platform":           platform,
            "platform_prefix":    _platform_prefix(platform),
            "platform_username":  f"{platform}:{unorm}",
            "url_domain":         _extract_domain(claim.url) if claim.url else "",
            "url_path_slug":      _extract_path_slug(claim.url) if claim.url else "",
            "email_hash":         e_hash,
            "source_tool":        claim.source_tool,
            "confidence":         claim.confidence,
            "tier":               claim.tier,
            "verified":           claim.verified,
            "url":                claim.url or "",
            "email":              claim.email or "",
            "phone":              claim.phone or "",
        })

    # Keep a stable schema even for empty inputs.
    df = pd.DataFrame(rows, columns=_EXPECTED_COLUMNS)

    # Splink requires a unique_id column — ensure no duplicates
    if not df.empty:
        df = df.drop_duplicates(subset=["unique_id"], keep="first")

    _validate_flatten_df(df)

    return df


# ── Normalisation helpers ─────────────────────────────────────────────────────


def _normalise_username(username: str) -> str:
    """Lowercase, strip special chars except alphanumeric and underscore."""
    return re.sub(r"[^a-z0-9_]", "", username.lower())


def _platform_prefix(platform: str) -> str:
    """Build a short, stable platform prefix used for blocking contracts."""
    p = (platform or "").lower().strip()
    if not p:
        return ""

    if p.startswith("github"):
        return "gh_"
    if p.startswith("twitter") or p == "x":
        return "tw_"
    if p.startswith("linkedin"):
        return "li_"

    if len(p) == 1:
        return p + "_"
    return p[:2] + "_"


def _extract_domain(url: str) -> str:
    """Extract and normalise the domain from a URL."""
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower().removeprefix("www.")
        return host
    except Exception:
        return ""


def _extract_path_slug(url: str) -> str:
    """Extract the first path segment from a URL."""
    try:
        path = urlparse(url).path.strip("/")
        return path.split("/")[0] if path else ""
    except Exception:
        return ""


def _hash_email(email: str | None) -> str:
    """HMAC-SHA256 hash of lowered email for privacy-preserving blocking."""
    if not email:
        return ""
    secret = os.getenv("PII_HMAC_SECRET", "change-me-pii-secret")
    value = email.strip().lower().encode()
    return hmac.new(secret.encode("utf-8"), value, hashlib.sha256).hexdigest()


def _validate_flatten_df(df: pd.DataFrame) -> None:
    """Fail fast if the Splink boundary schema drifts from expected contract."""
    missing = sorted(_REQUIRED_CONTRACT_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"flatten_claims missing required columns: {missing}")

    if df.empty:
        return

    if df["unique_id"].isna().any() or (df["unique_id"] == "").any():
        raise ValueError("flatten_claims produced empty unique_id values")

    if df["confidence"].isna().any() or not df["confidence"].between(0.0, 1.0).all():
        raise ValueError("flatten_claims confidence must be within [0.0, 1.0]")


def _make_unique_id(claim: IdentityClaim) -> str:
    """
    Deterministic unique ID for a single claim row.

    Hash of (platform, url, username, source_tool) so the same claim
    from the same tool always gets the same ID.
    """
    key = f"{claim.platform}|{claim.url or ''}|{claim.username}|{claim.source_tool}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]
