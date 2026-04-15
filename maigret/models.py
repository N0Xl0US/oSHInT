"""
models.py — Pydantic v2 schemas for every data boundary in the pipeline.

All inter-module communication uses these models instead of raw dicts.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


# ── Pipeline data ────────────────────────────────────────────────────────────


class RawProfile(BaseModel):
    """A claimed profile extracted from maigret NDJSON output."""

    platform: str
    url: Optional[str] = None
    username: str
    check_type: str = "unknown"
    source_http_status: int = 0
    has_ids: bool = False
    is_similar: bool = False

    @field_validator("platform", mode="before")
    @classmethod
    def _normalise_platform(cls, v: str) -> str:
        return v.lower().split(" ")[0] if v else "unknown"


class ScoredProfile(BaseModel):
    """A profile after PPF scoring — the primary pipeline output."""

    platform: str
    url: Optional[str] = None
    username: str
    tier: Literal["high", "medium", "low", "drop"]
    http_status: int = 0
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    is_api_endpoint: bool = False
    regions: list[str] = Field(default_factory=list)
    drop_reason: Optional[str] = None

    @field_validator("confidence", mode="before")
    @classmethod
    def _clamp_confidence(cls, v: float) -> float:
        try:
            v = float(v)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(1.0, round(v, 4)))


# ── Kafka event schema ──────────────────────────────────────────────────────


class EventProfile(BaseModel):
    """Normalised profile shape inside a Kafka event."""

    platform: str
    url: Optional[str] = None
    username: Optional[str] = None
    confidence: float = 0.0
    verified: bool = False
    tags: list[str] = Field(default_factory=list)


class QueryMeta(BaseModel):
    username: str
    source: str = "maigret"
    trigger: Literal["api", "scheduled", "test"] = "api"


class PipelineStats(BaseModel):
    total_raw: int = 0
    total_claimed: int = 0
    total_filtered: int = 0
    skipped_malformed: int = 0


class MaigretEvent(BaseModel):
    """Canonical ``osint.raw.maigret.v1`` Kafka event."""

    schema_version: str = "1.0"
    event_type: str = "osint.raw.maigret"
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    emitted_at: str = Field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())
    query: QueryMeta
    stats: PipelineStats
    profiles: list[EventProfile] = Field(default_factory=list)


# ── API response ─────────────────────────────────────────────────────────────


class PipelineMeta(BaseModel):
    total_raw: int
    total_claimed: int
    total_filtered: int
    kafka_topic: str
    kafka_status: str


class PipelineResponse(BaseModel):
    """Shape returned by ``/search_username_filtered``."""

    query: str
    pipeline: PipelineMeta
    profiles: list[ScoredProfile]


class SearchResponse(BaseModel):
    """Shape returned by ``/search_username``."""

    query: str
    total_scanned: int
    total_claimed: int
    profiles: list[RawProfile]


class ParseUrlResponse(BaseModel):
    url: str
    platform: str
    extracted_username: str


class HealthResponse(BaseModel):
    status: str = "ok"


class ReadyResponse(BaseModel):
    kafka: str
