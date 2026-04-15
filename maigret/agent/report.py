"""
report.py — Output models for the investigation agent.

Aligned with the Golden Record schema from handover.md §1.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


# ── Identity claims ──────────────────────────────────────────────────────────


class IdentityClaim(BaseModel):
    """A single platform presence claim discovered by the pipeline."""

    platform: str
    url: Optional[str] = None
    username: str
    email: Optional[str] = None
    email_hash: Optional[str] = None    # SHA256(lower(email)) for blocking
    phone: Optional[str] = None
    confidence: float = 0.0
    tier: Literal["high", "medium", "low", "drop"] = "medium"
    verified: bool = False
    source_tool: str = "maigret"
    institutions: list[str] = Field(default_factory=list)
    research_domains: list[str] = Field(default_factory=list)
    co_authors: list[str] = Field(default_factory=list)
    raw_data: dict[str, Any] | None = None


# ── Golden Record (handover.md §1 schema) ────────────────────────────────────


class Alias(BaseModel):
    type: Literal["email", "username", "phone", "handle"]
    value: str  # HMAC-SHA256 pseudonymised in production
    platform: Optional[str] = None
    confidence: float = 0.0
    source: str = "maigret"


class GoldenRecordAttributes(BaseModel):
    name_variants: list[str] = Field(default_factory=list)
    locations: list[str] = Field(default_factory=list)
    languages: list[str] = Field(default_factory=list)
    institutions: list[str] = Field(default_factory=list)
    research_domains: list[str] = Field(default_factory=list)
    co_authors: list[str] = Field(default_factory=list)


class SourceRecord(BaseModel):
    tool: str
    ingested_at: str = Field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())
    reliability_weight: float = 0.0


class MergeRecord(BaseModel):
    fragment_id: str
    merged_at: str = Field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())
    merge_score: float = 0.0


class QualityFlags(BaseModel):
    low_confidence: bool = False
    conflicting_data: bool = False
    quarantined: bool = False
    needs_review: bool = False


class DataGovernance(BaseModel):
    retention_expires_at: Optional[str] = None
    legal_basis: Literal["legitimate_interest", "public_interest", "consent"] = "legitimate_interest"
    erasure_requested: bool = False
    rectification_pending: bool = False


class GoldenRecord(BaseModel):
    """
    Canonical Golden Record per handover.md §1.

    Phase 1 produces mock/partial golden records from Splink output.
    Phase 2 (EvoKG) enriches these with temporal graph context.
    """

    entity_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: str = "1.0"
    resolved_at: str = Field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())
    confidence_score: float = 0.0
    resolution_method: Literal["deterministic", "probabilistic", "hybrid"] = "deterministic"

    aliases: list[Alias] = Field(default_factory=list)
    attributes: GoldenRecordAttributes = Field(default_factory=GoldenRecordAttributes)
    sources: list[SourceRecord] = Field(default_factory=list)
    merge_history: list[MergeRecord] = Field(default_factory=list)
    quality_flags: QualityFlags = Field(default_factory=QualityFlags)
    data_governance: DataGovernance = Field(default_factory=DataGovernance)


# ── Investigation report (agent output) ──────────────────────────────────────


class InvestigationReport(BaseModel):
    """
    Full output of an investigation run.

    Contains the LLM's narrative summary, structured claims bucketed by
    confidence, and the search provenance trail.
    """

    subject: str
    summary: str = ""
    platforms_found: int = 0
    high_confidence: list[IdentityClaim] = Field(default_factory=list)
    medium_confidence: list[IdentityClaim] = Field(default_factory=list)
    low_confidence: list[IdentityClaim] = Field(default_factory=list)
    search_history: list[str] = Field(default_factory=list)
    tool_calls_made: int = 0

    # Phase 1: partial golden record from current data
    golden_record: Optional[GoldenRecord] = None
