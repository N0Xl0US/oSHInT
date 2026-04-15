"""
interfaces.py — Abstract extension points for Phase 2 (Neo4j) and Phase 3 (EvoKG).

Phase 1 ships with in-memory / no-op defaults.
Phase 2 swaps in Neo4jGraphStore.
Phase 3 swaps in EvoKGResolver.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from maigret.agent.report import GoldenRecord, IdentityClaim


# ── Graph Store (Phase 2: Neo4j) ────────────────────────────────────────────


class GraphStore(ABC):
    """
    Abstract store for identity claims as a graph.

    Phase 2 implements this with Neo4j Community Edition.
    See handover.md §3 for node types and relationship schemas.
    """

    @abstractmethod
    async def store_claims(self, subject: str, claims: list[IdentityClaim]) -> int:
        """Store claims as graph nodes. Returns count of nodes created."""
        ...

    @abstractmethod
    async def get_claims(self, subject: str) -> list[IdentityClaim]:
        """Retrieve all claims for a subject."""
        ...

    @abstractmethod
    async def find_connections(self, subject: str) -> list[dict]:
        """Find relationships between a subject and other entities."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release resources."""
        ...


class InMemoryGraphStore(GraphStore):
    """Phase 1 default — stores claims in a dict keyed by subject."""

    def __init__(self) -> None:
        self._store: dict[str, list[IdentityClaim]] = {}

    async def store_claims(self, subject: str, claims: list[IdentityClaim]) -> int:
        existing = self._store.setdefault(subject, [])
        # Deduplicate by (platform, url)
        existing_keys = {(c.platform, c.url) for c in existing}
        new = [c for c in claims if (c.platform, c.url) not in existing_keys]
        existing.extend(new)
        return len(new)

    async def get_claims(self, subject: str) -> list[IdentityClaim]:
        return list(self._store.get(subject, []))

    async def find_connections(self, subject: str) -> list[dict]:
        return []  # No graph traversal in Phase 1

    async def close(self) -> None:
        self._store.clear()


# ── Entity Resolver (Phase 3: EvoKG) ────────────────────────────────────────


class ResolvedEntity:
    """Result of entity resolution — a cluster of claims about one person."""

    def __init__(self, entity_id: str, claims: list[IdentityClaim], confidence: float):
        self.entity_id = entity_id
        self.claims = claims
        self.confidence = confidence


class EntityResolver(ABC):
    """
    Abstract entity resolution interface.

    Phase 1: Splink deterministic/probabilistic linking.
    Phase 3: EvoKG temporal knowledge graph evolution.
    See handover.md §3 for conflict resolution rules.
    """

    @abstractmethod
    async def resolve(self, claims: list[IdentityClaim]) -> list[ResolvedEntity]:
        """Cluster claims that refer to the same real-world entity."""
        ...

    @abstractmethod
    async def merge(self, entity_a: str, entity_b: str) -> str:
        """Merge two entities. Returns surviving entity_id."""
        ...


class NoOpEntityResolver(EntityResolver):
    """Phase 1 default — treats all claims as one entity (no resolution)."""

    async def resolve(self, claims: list[IdentityClaim]) -> list[ResolvedEntity]:
        if not claims:
            return []
        return [ResolvedEntity(
            entity_id="unresolved",
            claims=claims,
            confidence=sum(c.confidence for c in claims) / len(claims) if claims else 0.0,
        )]

    async def merge(self, entity_a: str, entity_b: str) -> str:
        return entity_a


# ── OSINT Tool Interface (for Maigret, Academic, GitHub, etc.) ───────────────


class OsintTool(ABC):
    """
    Abstract interface for pluggable OSINT extraction tools.

    Phase 1: Maigret (implemented).
    Future: TheHarvester, Recon-ng, etc.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Tool identifier (e.g. 'maigret', 'academic')."""
        ...

    @abstractmethod
    async def search(self, username: str, **kwargs: Any) -> list[IdentityClaim]:
        """Run a search and return claims."""
        ...

    @abstractmethod
    def get_tool_schema(self) -> dict:
        """Return the Groq function-calling JSON schema for this tool."""
        ...
