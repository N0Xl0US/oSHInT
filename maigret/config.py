"""
config.py — Centralised application settings.

All configuration is loaded from environment variables (with .env support).
Use ``get_settings()`` everywhere; it returns a cached singleton.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Production configuration for the Maigret MCP server."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Kafka ───────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "osint.raw.maigret.v1"
    kafka_probe_topic: str = "osint.probe.maigret.v1"
    kafka_acks: str = "all"
    kafka_compression: str = "snappy"
    kafka_retries: int = 5
    kafka_retry_backoff_ms: int = 300
    kafka_linger_ms: int = 10
    kafka_batch_num_messages: int = 100
    kafka_idempotence: bool = True
    kafka_flush_timeout: float = 15.0

    # ── Maigret subprocess ──────────────────────────────────────────────────
    maigret_binary: str = "maigret"
    maigret_timeout: int = 10
    maigret_retries: int = 1
    maigret_subprocess_timeout: int = 120  # total wall-clock seconds

    # ── PPF ──────────────────────────────────────────────────────────────────
    ppf_default_min_confidence: float = 0.3
    ppf_max_concurrency: int = 20
    ppf_http_timeout: int = 5
    ppf_verify_http: bool = False

    # ── Logging ─────────────────────────────────────────────────────────────
    log_level: str = "INFO"
    log_format: Literal["json", "human"] = "human"

    # ── Server ──────────────────────────────────────────────────────────────
    server_host: str = "0.0.0.0"
    server_port: int = 8000


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton of application settings."""
    return Settings()
