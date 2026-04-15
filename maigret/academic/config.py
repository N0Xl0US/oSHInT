"""
academic/config.py — Configuration for academic research paper lookup.

Settings loaded from environment variables with .env support.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AcademicConfig(BaseSettings):
    """Configuration for academic data source clients."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        populate_by_name=True,
        extra="ignore",
    )

    # ── Semantic Scholar ─────────────────────────────────────────────────
    semantic_scholar_base_url: str = "https://api.semanticscholar.org/graph/v1"

    # ── OpenAlex ─────────────────────────────────────────────────────────
    openalex_base_url: str = "https://api.openalex.org"
    openalex_mailto: str = ""  # Set to get into the polite pool

    # ── Orchid / ORCID ──────────────────────────────────────────────────
    orchid_base_url: str = "https://pub.orcid.org/v3.0"
    orchid_token_url: str = "https://orcid.org/oauth/token"
    orchid_scope: str = "/read-public"
    orchid_client_id: str = Field(
        default="",
        validation_alias=AliasChoices(
            "ORCHID_Client_ID",
            "ORCHID_CLIENT_ID",
            "ORCID_CLIENT_ID",
        ),
    )
    orchid_client_secret: str = Field(
        default="",
        validation_alias=AliasChoices(
            "ORCHID_Client_Secret",
            "ORCHID_CLIENT_SECRET",
            "ORCID_CLIENT_SECRET",
        ),
    )

    # ── Shared ───────────────────────────────────────────────────────────
    academic_request_timeout: int = 15
    academic_max_authors: int = 5
    academic_min_confidence: float = 0.3
    academic_kafka_topic: str = "osint.raw.academic.v1"

    @property
    def orchid_configured(self) -> bool:
        """True when Orchid member credentials are available."""
        return bool(self.orchid_client_id and self.orchid_client_secret)


@lru_cache(maxsize=1)
def get_academic_config() -> AcademicConfig:
    """Return a cached singleton of academic settings."""
    return AcademicConfig()
