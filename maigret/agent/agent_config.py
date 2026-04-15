"""
agent_config.py — Configuration for the investigation agent.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class AgentConfig(BaseSettings):
    """Settings for the Groq-powered investigation agent."""

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parents[2] / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Groq LLM ────────────────────────────────────────────────────────────
    groq_api_key: str
    groq_model: str = "llama-3.3-70b-versatile"
    groq_temperature: float = 0.1
    groq_max_tokens: int = 1024

    # ── OSINT server ────────────────────────────────────────────────────────
    osint_server_url: str = "http://localhost:8000"
    request_timeout: int = 300

    # ── Agent behaviour ─────────────────────────────────────────────────────
    max_tool_rounds: int = 5
    max_total_searches: int = 10
    max_requery_attempts: int = 1

    # ── Golden Record thresholds (from handover.md §1) ──────────────────────
    confidence_direct_ingest: float = 0.85
    confidence_flag_ingest: float = 0.60
    confidence_quarantine: float = 0.40
    # Below 0.40 → block


@lru_cache(maxsize=1)
def get_agent_config() -> AgentConfig:
    """Return a cached singleton of agent settings.

    Prefer the repository root .env as the single source of truth.
    """
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        try:
            return AgentConfig(_env_file=str(env_path))
        except ValidationError:
            pass

    return AgentConfig()
