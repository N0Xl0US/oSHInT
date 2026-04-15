"""
academic/runner.py — AcademicSearchTool: OsintTool implementation.

Implements the OsintTool interface from maigret.agent.interfaces, so the
InvestigationAgent can call it transparently alongside Maigret and other sources.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from maigret.academic.client import OpenAlexClient, OrchidClient, SemanticScholarClient
from maigret.academic.config import AcademicConfig, get_academic_config
from maigret.academic.event_builder import publish_academic_event
from maigret.academic.models import (
    AcademicAuthorProfile,
    AcademicPaper,
    AcademicSearchResult,
    CoAuthor,
)
from maigret.academic.ppf import score_and_merge
from maigret.agent.interfaces import OsintTool
from maigret.agent.report import IdentityClaim

logger = logging.getLogger(__name__)


# ── Raw response → model conversion ─────────────────────────────────────────


def _parse_ss_author(raw: dict[str, Any]) -> AcademicAuthorProfile | None:
    """Convert a Semantic Scholar author dict to an AcademicAuthorProfile."""
    author_id = raw.get("authorId")
    name = raw.get("name")
    if not author_id or not name:
        return None

    # Extract ORCID from externalIds
    external_ids = raw.get("externalIds") or {}
    orcid = external_ids.get("ORCID")

    # Parse inline papers
    papers: list[AcademicPaper] = []
    for p in (raw.get("papers") or [])[:5]:
        papers.append(AcademicPaper(
            title=p.get("title") or "Untitled",
            year=p.get("year"),
            doi=(p.get("externalIds") or {}).get("DOI"),
            venue=p.get("venue"),
            citation_count=p.get("citationCount") or 0,
            source="semantic_scholar",
        ))

    return AcademicAuthorProfile(
        source="semantic_scholar",
        author_id=str(author_id),
        name=name,
        affiliations=raw.get("affiliations") or [],
        orcid=orcid,
        homepage=raw.get("homepage"),
        paper_count=raw.get("paperCount") or 0,
        citation_count=raw.get("citationCount") or 0,
        h_index=raw.get("hIndex"),
        top_papers=papers,
    )


def _parse_oa_author(raw: dict[str, Any]) -> AcademicAuthorProfile | None:
    """Convert an OpenAlex author dict to an AcademicAuthorProfile."""
    oa_id = raw.get("id")
    name = raw.get("display_name")
    if not oa_id or not name:
        return None

    # ORCID comes as a full URL: "https://orcid.org/0000-0001-2345-6789"
    orcid_raw = raw.get("orcid") or ""
    orcid = orcid_raw.replace("https://orcid.org/", "").strip() or None

    # Build affiliations list
    affiliations: list[str] = []
    last_inst = raw.get("last_known_institution") or {}
    if last_inst.get("display_name"):
        affiliations.append(last_inst["display_name"])
    for aff in raw.get("affiliations") or []:
        inst = aff.get("institution") or {}
        inst_name = inst.get("display_name")
        if inst_name and inst_name not in affiliations:
            affiliations.append(inst_name)

    # h-index from summary_stats
    summary = raw.get("summary_stats") or {}
    h_index = summary.get("h_index")

    research_domains = _extract_oa_research_domains(raw)
    co_authors = _extract_oa_coauthors(raw)

    return AcademicAuthorProfile(
        source="openalex",
        author_id=str(oa_id),
        name=name,
        affiliations=affiliations,
        orcid=orcid,
        paper_count=raw.get("works_count") or 0,
        citation_count=raw.get("cited_by_count") or 0,
        h_index=int(h_index) if h_index is not None else None,
        research_domains=research_domains,
        co_authors=co_authors,
    )


def _parse_orchid_author(raw: dict[str, Any]) -> AcademicAuthorProfile | None:
    """Convert an Orchid/ORCID author dict to an AcademicAuthorProfile."""

    # Expanded-search payload uses dashed keys; search payload uses nested dicts.
    orcid = (
        raw.get("orcid-id")
        or raw.get("orcid")
        or ((raw.get("orcid-identifier") or {}).get("path"))
    )

    given = raw.get("given-names") or raw.get("given_name")
    family = raw.get("family-names") or raw.get("family_name")
    credit = raw.get("credit-name") or raw.get("credit_name")

    name_parts = [str(x).strip() for x in (given, family) if x]
    name = " ".join(name_parts).strip() or (str(credit).strip() if credit else "")
    if not name:
        return None

    affiliations: list[str] = []
    seen_affiliations: set[str] = set()

    raw_affiliations = (
        raw.get("institution-name")
        or raw.get("institution_names")
        or raw.get("institution")
        or []
    )
    if isinstance(raw_affiliations, str):
        raw_affiliations = [raw_affiliations]

    for item in raw_affiliations:
        value = ""
        if isinstance(item, str):
            value = item.strip()
        elif isinstance(item, dict):
            value = str(
                item.get("name")
                or item.get("value")
                or item.get("institution-name")
                or ""
            ).strip()
        if value:
            key = value.lower()
            if key not in seen_affiliations:
                seen_affiliations.add(key)
                affiliations.append(value)

    research_domains: list[str] = []
    seen_domains: set[str] = set()
    raw_keywords = raw.get("keyword") or raw.get("keywords") or []
    if isinstance(raw_keywords, str):
        raw_keywords = [raw_keywords]
    for item in raw_keywords:
        value = item.strip() if isinstance(item, str) else ""
        if not value and isinstance(item, dict):
            value = str(item.get("content") or item.get("value") or "").strip()
        if value:
            key = value.lower()
            if key not in seen_domains:
                seen_domains.add(key)
                research_domains.append(value)

    orcid_clean = str(orcid).strip() if orcid else None
    author_id = f"https://orcid.org/{orcid_clean}" if orcid_clean else f"orchid:{name.lower()}"

    return AcademicAuthorProfile(
        source="orchid",
        author_id=author_id,
        name=name,
        affiliations=affiliations,
        orcid=orcid_clean,
        research_domains=research_domains,
    )


def _extract_oa_research_domains(raw: dict[str, Any]) -> list[str]:
    """Extract normalised research domains from OpenAlex author payloads."""
    names: list[str] = []
    seen: set[str] = set()

    def _push(name: str | None) -> None:
        if not name:
            return
        clean = str(name).strip()
        if not clean:
            return
        key = clean.lower()
        if key in seen:
            return
        seen.add(key)
        names.append(clean)

    for item in raw.get("research_domains") or []:
        if isinstance(item, str):
            _push(item)
            continue
        if isinstance(item, dict):
            _push(item.get("display_name") or item.get("name"))
            domain = item.get("domain")
            if isinstance(domain, dict):
                _push(domain.get("display_name") or domain.get("name"))

    for item in raw.get("x_concepts") or []:
        if isinstance(item, str):
            _push(item)
            continue
        if isinstance(item, dict):
            _push(item.get("display_name") or item.get("name"))

    for item in raw.get("topics") or []:
        if isinstance(item, str):
            _push(item)
            continue
        if isinstance(item, dict):
            _push(item.get("display_name") or item.get("name"))
            domain = item.get("domain")
            if isinstance(domain, dict):
                _push(domain.get("display_name") or domain.get("name"))

    return names[:20]


def _extract_oa_coauthors(raw: dict[str, Any]) -> list[CoAuthor]:
    """Extract co-authors from OpenAlex author payloads when present."""
    result: list[CoAuthor] = []
    seen: set[str] = set()

    for entry in raw.get("co_authors") or raw.get("coauthors") or []:
        name: str | None = None
        author_id: str | None = None

        if isinstance(entry, str):
            name = entry
        elif isinstance(entry, dict):
            author = entry.get("author")
            if isinstance(author, dict):
                name = author.get("display_name") or author.get("name")
                author_id = author.get("id") or author.get("author_id")

            name = name or entry.get("display_name") or entry.get("name")
            author_id = author_id or entry.get("id") or entry.get("author_id")

        if not name:
            continue

        key = str(author_id).strip().lower() if author_id else str(name).strip().lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(CoAuthor(
            name=str(name).strip(),
            author_id=str(author_id).strip() if author_id else None,
            source="openalex",
        ))

    return result[:20]


def _parse_oa_works(raw_works: list[dict[str, Any]]) -> list[AcademicPaper]:
    """Convert OpenAlex work dicts to AcademicPaper models."""
    papers: list[AcademicPaper] = []
    for w in raw_works:
        venue = None
        loc = w.get("primary_location") or {}
        src = loc.get("source") or {}
        venue = src.get("display_name")

        doi_raw = w.get("doi") or ""
        doi = doi_raw.replace("https://doi.org/", "").strip() or None

        papers.append(AcademicPaper(
            title=w.get("title") or "Untitled",
            year=w.get("publication_year"),
            doi=doi,
            venue=venue,
            citation_count=w.get("cited_by_count") or 0,
            source="openalex",
        ))
    return papers


def _extract_coauthors_from_papers(
    papers_data: list[dict[str, Any]],
    exclude_author_id: str,
) -> list[CoAuthor]:
    """Extract unique co-authors from Semantic Scholar paper author lists."""
    seen: set[str] = set()
    coauthors: list[CoAuthor] = []
    for paper in papers_data:
        for author in (paper.get("authors") or []):
            aid = str(author.get("authorId") or "")
            name = author.get("name") or ""
            if not name or aid == exclude_author_id:
                continue
            key = aid or name.lower()
            if key in seen:
                continue
            seen.add(key)
            coauthors.append(CoAuthor(
                name=name,
                author_id=aid or None,
                source="semantic_scholar",
            ))
    return coauthors[:20]  # Cap at 20 co-authors


# ── Main Tool ────────────────────────────────────────────────────────────────


class AcademicSearchTool(OsintTool):
    """
    OsintTool implementation for academic research paper lookup.

    Queries Semantic Scholar and OpenAlex in parallel. Optionally adds Orchid
    when member credentials are configured, then merges all results
    via the academic PPF, and returns scored author profiles.

    Usage:
        tool = AcademicSearchTool()
        result = await tool.search_academic("Jane Smith", institution="MIT")
    """

    def __init__(self, config: AcademicConfig | None = None) -> None:
        self._config = config or get_academic_config()

    @property
    def name(self) -> str:
        return "academic"

    # ── OsintTool interface ───────────────────────────────────────────────

    async def search(self, username: str, **kwargs: Any) -> list[IdentityClaim]:
        """
        Run an academic search and return IdentityClaim objects.

        The ``username`` param is treated as the author name.
        kwargs: institution (str), min_confidence (float).
        """
        institution = kwargs.get("institution")
        min_conf = float(kwargs.get("min_confidence", self._config.academic_min_confidence))

        result = await self.search_academic(
            name=username,
            institution=institution,
            min_confidence=min_conf,
        )

        # Convert AcademicAuthorProfiles → IdentityClaims
        claims: list[IdentityClaim] = []
        for p in result.profiles:
            url = None
            if p.source == "semantic_scholar":
                url = f"https://www.semanticscholar.org/author/{p.author_id}"
            elif p.source == "openalex":
                url = p.author_id if p.author_id.startswith("http") else None
            elif p.source == "orchid" and p.orcid:
                url = f"https://orcid.org/{p.orcid}"

            claims.append(IdentityClaim(
                platform=p.source,
                url=url,
                username=p.name,
                confidence=p.confidence,
                tier="high" if p.confidence >= 0.7 else "medium" if p.confidence >= 0.4 else "low",
                verified=p.orcid is not None,
                source_tool="academic",
                institutions=list(dict.fromkeys(p.affiliations)),
                research_domains=list(dict.fromkeys(p.research_domains)),
                co_authors=list(dict.fromkeys(ca.name for ca in p.co_authors if ca.name)),
            ))
        return claims

    def get_tool_schema(self) -> dict:
        """Return the Groq function-calling JSON schema for this tool."""
        return {
            "type": "function",
            "function": {
                "name": "search_academic",
                "description": (
                    "Search for academic publications authored by a person. "
                    "Returns author profiles from Semantic Scholar, OpenAlex, "
                    "and Orchid (when configured) "
                    "with affiliations, ORCID, top papers, co-author networks, "
                    "and confidence scores. Use this to find a subject's academic "
                    "research footprint."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Full name of the author to search for (e.g. 'Jane Smith')",
                        },
                        "institution": {
                            "type": "string",
                            "description": "Optional institution or university to narrow results (e.g. 'MIT')",
                        },
                    },
                    "required": ["name"],
                },
            },
        }

    # ── High-level search method ──────────────────────────────────────────

    async def search_academic(
        self,
        name: str,
        institution: str | None = None,
        min_confidence: float | None = None,
    ) -> AcademicSearchResult:
        """
        Full academic search: query both sources, enrich, score, merge.

        Returns:
            AcademicSearchResult with scored, deduplicated profiles.
        """
        min_conf = min_confidence if min_confidence is not None else self._config.academic_min_confidence
        max_authors = self._config.academic_max_authors

        logger.info(
            "AcademicSearchTool.search_academic: name='%s' institution='%s'",
            name, institution,
        )

        # Step 1: Query configured sources in parallel
        tasks: list[tuple[str, Any]] = [
            ("semantic_scholar", self._fetch_semantic_scholar(name, max_authors)),
            ("openalex", self._fetch_openalex(name, max_authors)),
        ]
        if self._config.orchid_configured:
            tasks.append(("orchid", self._fetch_orchid(name, max_authors)))

        source_results = await asyncio.gather(*(task for _, task in tasks))
        profiles_by_source: dict[str, list[AcademicAuthorProfile]] = {
            source_name: profiles
            for (source_name, _), profiles in zip(tasks, source_results)
        }

        ss_profiles = profiles_by_source.get("semantic_scholar", [])
        oa_profiles = profiles_by_source.get("openalex", [])
        orchid_profiles = profiles_by_source.get("orchid", [])

        all_profiles = ss_profiles + oa_profiles + orchid_profiles
        total_found = len(all_profiles)
        sources_queried = len(tasks)

        logger.info(
            "  Raw results: semantic_scholar=%d openalex=%d orchid=%d",
            len(ss_profiles), len(oa_profiles),
            len(orchid_profiles),
        )

        # Step 2: PPF scoring and merge
        scored = score_and_merge(
            profiles=all_profiles,
            query_name=name,
            query_institution=institution,
            min_confidence=min_conf,
        )

        logger.info(
            "AcademicSearchTool.search_academic complete: name='%s' profiles=%d",
            name, len(scored),
        )

        # Publish raw event to osint.raw.academic.v1 (fire-and-forget)
        publish_academic_event(
            query_name=name,
            profiles=scored,
            query_institution=institution,
            sources_queried=sources_queried,
            trigger="agent",
        )

        return AcademicSearchResult(
            query_name=name,
            query_institution=institution,
            profiles=scored,
            total_sources_queried=sources_queried,
            total_authors_found=total_found,
        )

    # ── Private fetch helpers ─────────────────────────────────────────────

    async def _fetch_semantic_scholar(
        self, name: str, limit: int
    ) -> list[AcademicAuthorProfile]:
        """Fetch authors + co-authors from Semantic Scholar."""
        profiles: list[AcademicAuthorProfile] = []
        try:
            async with SemanticScholarClient(self._config) as client:
                raw_authors = await client.search_authors(name, limit=limit)
                for raw in raw_authors:
                    profile = _parse_ss_author(raw)
                    if profile is None:
                        continue

                    # Enrich with co-authors from papers
                    papers_data = await client.get_author_papers(
                        profile.author_id, limit=10
                    )
                    profile.co_authors = _extract_coauthors_from_papers(
                        papers_data, exclude_author_id=profile.author_id,
                    )
                    profiles.append(profile)
        except Exception as exc:
            logger.error("Semantic Scholar fetch failed: %s", exc)
        return profiles

    async def _fetch_openalex(
        self, name: str, limit: int
    ) -> list[AcademicAuthorProfile]:
        """Fetch authors + top works from OpenAlex."""
        profiles: list[AcademicAuthorProfile] = []
        try:
            async with OpenAlexClient(self._config) as client:
                raw_authors = await client.search_authors(name, limit=limit)
                for raw in raw_authors:
                    profile = _parse_oa_author(raw)
                    if profile is None:
                        continue

                    # Enrich with top works
                    raw_works = await client.get_top_works(
                        profile.author_id, limit=5
                    )
                    profile.top_papers = _parse_oa_works(raw_works)
                    profiles.append(profile)
        except Exception as exc:
            logger.error("OpenAlex fetch failed: %s", exc)
        return profiles

    async def _fetch_orchid(
        self, name: str, limit: int
    ) -> list[AcademicAuthorProfile]:
        """Fetch author profiles from Orchid/ORCID."""
        profiles: list[AcademicAuthorProfile] = []
        try:
            async with OrchidClient(self._config) as client:
                raw_authors = await client.search_researchers(name, limit=limit)
                for raw in raw_authors:
                    profile = _parse_orchid_author(raw)
                    if profile is not None:
                        profiles.append(profile)
        except Exception as exc:
            logger.error("Orchid fetch failed: %s", exc)
        return profiles
