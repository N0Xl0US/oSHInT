"""
academic/models.py — Pydantic models for academic research output.

Structured representations of author profiles, papers, and co-authors
returned by the Semantic Scholar and OpenAlex APIs.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class AcademicPaper(BaseModel):
    """A single academic publication."""

    title: str
    year: Optional[int] = None
    doi: Optional[str] = None
    venue: Optional[str] = None
    citation_count: int = 0
    source: str = "unknown"  # "semantic_scholar" | "openalex"


class CoAuthor(BaseModel):
    """A co-author discovered from publication metadata."""

    name: str
    author_id: Optional[str] = None
    source: str = "unknown"


class AcademicAuthorProfile(BaseModel):
    """A resolved academic author profile from a single source."""

    source: str  # "semantic_scholar" | "openalex"
    author_id: str
    name: str
    affiliations: list[str] = Field(default_factory=list)
    orcid: Optional[str] = None
    homepage: Optional[str] = None
    paper_count: int = 0
    citation_count: int = 0
    h_index: Optional[int] = None
    research_domains: list[str] = Field(default_factory=list)
    top_papers: list[AcademicPaper] = Field(default_factory=list)
    co_authors: list[CoAuthor] = Field(default_factory=list)
    confidence: float = 0.0


class AcademicSearchResult(BaseModel):
    """Top-level result returned by the academic search tool."""

    query_name: str
    query_institution: Optional[str] = None
    profiles: list[AcademicAuthorProfile] = Field(default_factory=list)
    total_sources_queried: int = 0
    total_authors_found: int = 0
