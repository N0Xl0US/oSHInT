"""
resolver/config.py — Configuration for Splink entity resolution.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class ResolverConfig(BaseSettings):
    """Settings for the Splink entity resolution pipeline."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Splink parameters ────────────────────────────────────────────────
    match_threshold: float = 0.85        # conservative threshold with verified claims
    cluster_threshold: float = 0.75      # conservative cluster threshold
    min_claims_for_splink: int = 5       # below this, use deterministic fallback
    low_signal_max_claims: int = 60
    low_signal_max_unique_usernames: int = 3
    low_signal_dominant_username_ratio: float = 0.85
    low_signal_suppress_splink_warnings: bool = True
    low_signal_u_max_pairs: int = 20_000

    # ── Golden Record thresholds (handover.md §1) ────────────────────────
    confidence_direct_ingest: float = 0.85
    confidence_flag_ingest: float = 0.60
    confidence_quarantine: float = 0.40

    # ── Kafka ────────────────────────────────────────────────────────────
    resolved_topic: str = "identity.resolved"
    quarantine_topic: str = "identity.quarantine"

    # ── Privacy ──────────────────────────────────────────────────────────
    pii_hmac_secret: str = "change-me-pii-secret"


@lru_cache(maxsize=1)
def get_resolver_config() -> ResolverConfig:
    """Return a cached singleton of resolver settings."""
    return ResolverConfig()
