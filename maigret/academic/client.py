"""
academic/client.py — Async HTTP clients for Semantic Scholar, OpenAlex, and Orchid.

All clients are async context managers with rate-limit awareness.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from maigret.academic.config import AcademicConfig, get_academic_config

logger = logging.getLogger(__name__)


# ── Semantic Scholar ─────────────────────────────────────────────────────────


_SS_AUTHOR_FIELDS = (
    "authorId,name,affiliations,homepage,paperCount,citationCount,"
    "hIndex,externalIds,"
    "papers.title,papers.year,papers.externalIds,"
    "papers.citationCount,papers.venue"
)

_SS_COAUTHOR_FIELDS = "authors"


class SemanticScholarClient:
    """Async client for the Semantic Scholar Academic Graph API."""

    def __init__(self, config: AcademicConfig | None = None) -> None:
        self._config = config or get_academic_config()
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "SemanticScholarClient":
        self._client = httpx.AsyncClient(
            base_url=self._config.semantic_scholar_base_url,
            timeout=self._config.academic_request_timeout,
            headers={"Accept": "application/json"},
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def search_authors(self, name: str, limit: int = 5) -> list[dict[str, Any]]:
        """
        Search for authors by name.

        GET /author/search?query=<name>&fields=...&limit=<limit>
        """
        assert self._client is not None, "Use as async context manager"
        try:
            resp = await self._client.get(
                "/author/search",
                params={"query": name, "fields": _SS_AUTHOR_FIELDS, "limit": limit},
            )
            if resp.status_code == 429:
                logger.warning("Semantic Scholar rate limit hit (429)")
                return []
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", [])
        except httpx.HTTPStatusError as exc:
            logger.error("Semantic Scholar author search failed: %s", exc)
            return []
        except Exception as exc:
            logger.error("Semantic Scholar request error: %s", exc)
            return []

    async def get_author_papers(
        self, author_id: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        """
        Fetch papers for a specific author (used for co-author extraction).

        GET /author/{authorId}/papers?fields=authors&limit=<limit>
        """
        assert self._client is not None, "Use as async context manager"
        try:
            resp = await self._client.get(
                f"/author/{author_id}/papers",
                params={"fields": _SS_COAUTHOR_FIELDS, "limit": limit},
            )
            if resp.status_code == 429:
                logger.warning("Semantic Scholar rate limit hit (429)")
                return []
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", [])
        except httpx.HTTPStatusError as exc:
            logger.error("Semantic Scholar papers fetch failed: %s", exc)
            return []
        except Exception as exc:
            logger.error("Semantic Scholar request error: %s", exc)
            return []


# ── OpenAlex ─────────────────────────────────────────────────────────────────


_OA_AUTHOR_SELECT = (
    "id,display_name,orcid,affiliations,works_count,"
    "cited_by_count,summary_stats,last_known_institution,x_concepts,topics"
)

_OA_WORKS_SELECT = (
    "title,publication_year,doi,primary_location,cited_by_count"
)


class OpenAlexClient:
    """Async client for the OpenAlex API."""

    def __init__(self, config: AcademicConfig | None = None) -> None:
        self._config = config or get_academic_config()
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "OpenAlexClient":
        params: dict[str, str] = {}
        if self._config.openalex_mailto:
            params["mailto"] = self._config.openalex_mailto
        self._client = httpx.AsyncClient(
            base_url=self._config.openalex_base_url,
            timeout=self._config.academic_request_timeout,
            params=params,
            headers={"Accept": "application/json"},
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def search_authors(self, name: str, limit: int = 5) -> list[dict[str, Any]]:
        """
        Search for authors by name.

        GET /authors?search=<name>&per-page=<limit>&select=...
        """
        assert self._client is not None, "Use as async context manager"
        params = {
            "search": name,
            "per-page": limit,
            "select": _OA_AUTHOR_SELECT,
        }
        try:
            resp = await self._client.get("/authors", params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 400:
                try:
                    retry_resp = await self._client.get(
                        "/authors",
                        params={"search": name, "per-page": limit},
                    )
                    retry_resp.raise_for_status()
                    retry_data = retry_resp.json()
                    return retry_data.get("results", [])
                except Exception as retry_exc:
                    logger.error("OpenAlex author search retry failed: %s", retry_exc)
            logger.error("OpenAlex author search failed: %s", exc)
            return []
        except Exception as exc:
            logger.error("OpenAlex request error: %s", exc)
            return []

    async def get_top_works(
        self, author_id: str, limit: int = 5
    ) -> list[dict[str, Any]]:
        """
        Fetch top-cited works for an author.

        GET /works?filter=author.id:<oa_id>&sort=cited_by_count:desc&per-page=<limit>&select=...
        """
        assert self._client is not None, "Use as async context manager"
        params = {
            "filter": f"author.id:{author_id}",
            "sort": "cited_by_count:desc",
            "per-page": limit,
            "select": _OA_WORKS_SELECT,
        }
        try:
            resp = await self._client.get("/works", params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 400:
                try:
                    retry_resp = await self._client.get(
                        "/works",
                        params={
                            "filter": f"author.id:{author_id}",
                            "sort": "cited_by_count:desc",
                            "per-page": limit,
                        },
                    )
                    retry_resp.raise_for_status()
                    retry_data = retry_resp.json()
                    return retry_data.get("results", [])
                except Exception as retry_exc:
                    logger.error("OpenAlex works fetch retry failed: %s", retry_exc)
            logger.error("OpenAlex works fetch failed: %s", exc)
            return []
        except Exception as exc:
            logger.error("OpenAlex request error: %s", exc)
            return []


# ── Orchid / ORCID ──────────────────────────────────────────────────────────


class OrchidClient:
    """Async client for Orchid (ORCID) member/public API search."""

    def __init__(self, config: AcademicConfig | None = None) -> None:
        self._config = config or get_academic_config()
        self._client: httpx.AsyncClient | None = None
        self._token: str | None = None

    async def __aenter__(self) -> "OrchidClient":
        self._client = httpx.AsyncClient(
            base_url=self._config.orchid_base_url,
            timeout=self._config.academic_request_timeout,
            headers={
                "Accept": "application/vnd.orcid+json",
            },
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _get_access_token(self, force_refresh: bool = False) -> str | None:
        """Fetch and cache OAuth token when member credentials are configured."""
        assert self._client is not None, "Use as async context manager"
        if self._token and not force_refresh:
            return self._token
        if not self._config.orchid_configured:
            return None

        try:
            resp = await self._client.post(
                self._config.orchid_token_url,
                data={
                    "client_id": self._config.orchid_client_id,
                    "client_secret": self._config.orchid_client_secret,
                    "grant_type": "client_credentials",
                    "scope": self._config.orchid_scope,
                },
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            token = (resp.json() or {}).get("access_token")
            self._token = str(token) if token else None
            return self._token
        except Exception as exc:
            logger.error("Orchid token request failed: %s", exc)
            self._token = None
            return None

    async def search_researchers(self, name: str, limit: int = 5) -> list[dict[str, Any]]:
        """
        Search Orchid/ORCID researchers by name.

        Primary endpoint:
          GET /expanded-search/?q=<name>&start=0&rows=<limit>
        Fallback endpoint:
          GET /search/?q=<name>
        """
        assert self._client is not None, "Use as async context manager"

        auth_headers: dict[str, str] = {}
        token = await self._get_access_token()
        if token:
            auth_headers["Authorization"] = f"Bearer {token}"

        params = {
            "q": name,
            "start": 0,
            "rows": limit,
        }

        try:
            resp = await self._client.get(
                "/expanded-search/",
                params=params,
                headers=auth_headers or None,
            )
            if resp.status_code == 401 and token:
                refresh = await self._get_access_token(force_refresh=True)
                if refresh:
                    retry_headers = {"Authorization": f"Bearer {refresh}"}
                    resp = await self._client.get(
                        "/expanded-search/",
                        params=params,
                        headers=retry_headers,
                    )

            if resp.status_code in (400, 404):
                retry_resp = await self._client.get(
                    "/search/",
                    params={"q": name},
                    headers=auth_headers or None,
                )
                retry_resp.raise_for_status()
                retry_data = retry_resp.json() or {}
                return retry_data.get("result", [])

            resp.raise_for_status()
            data = resp.json() or {}
            if "expanded-result" in data:
                return data.get("expanded-result", [])
            return data.get("result", [])
        except Exception as exc:
            logger.error("Orchid researcher search failed: %s", exc)
            return []
