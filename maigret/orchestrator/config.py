"""
orchestrator/config.py — Configuration for the identity orchestrator.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class OrchestratorConfig(BaseSettings):
    """Settings for the fan-out / resolve / publish pipeline."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Per-source timeouts (seconds) ────────────────────────────────────
    maigret_timeout: int = 120         # maigret subprocess is slow
    academic_timeout: int = 30         # academic APIs are fast
    github_timeout: int = 20               # GitHub API is usually fast
    github_octosuite_timeout: int = 45     # 4 parallel API calls; allow buffer
    blackbird_timeout: int = 240       # Blackbird may scan hundreds of sites
    spiderfoot_timeout: int = 120      # SpiderFoot username scans can take time
    holehe_timeout: int = 45           # Holehe checks many sites concurrently
    h8mail_timeout: int = 60           # h8mail can run breach lookups for longer
    linkedin_timeout: int = 45         # LinkedIn profile scrape is single-target but can be slow

    # ── Confidence thresholds (passed to sources) ────────────────────────
    min_confidence_maigret: float = 0.3
    min_confidence_academic: float = 0.3
    min_confidence_github: float = 0.35
    min_confidence_github_octosuite: float = 0.35
    min_confidence_blackbird: float = 0.45
    min_confidence_spiderfoot: float = 0.45
    min_confidence_holehe: float = 0.45
    min_confidence_h8mail: float = 0.45
    min_confidence_linkedin: float = 0.45

    # ── Kafka ────────────────────────────────────────────────────────────
    resolved_topic: str = "identity.resolved"


@lru_cache(maxsize=1)
def get_orchestrator_config() -> OrchestratorConfig:
    """Return a cached singleton of orchestrator settings."""
    return OrchestratorConfig()
