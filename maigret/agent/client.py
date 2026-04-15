"""
client.py — Async HTTP client wrapping the OSINT FastAPI server.

Uses httpx.AsyncClient with connection pooling and retries.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from maigret.agent.agent_config import AgentConfig

logger = logging.getLogger(__name__)


class OsintClientError(Exception):
    """Raised when the OSINT server returns an error."""


class OsintClient:
    """
    Async HTTP client for the Maigret OSINT FastAPI server.

    Usage:
        async with OsintClient(config) as client:
            result = await client.search_username_filtered("testuser")
    """

    def __init__(self, config: AgentConfig) -> None:
        self._base_url = config.osint_server_url.rstrip("/")
        self._timeout = config.request_timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=httpx.Timeout(self._timeout, connect=10),
                limits=httpx.Limits(max_connections=10),
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "OsintClient":
        await self._ensure_client()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # ── API methods ──────────────────────────────────────────────────────

    async def health_check(self) -> bool:
        """Check if the OSINT server is reachable."""
        try:
            client = await self._ensure_client()
            resp = await client.get("/health")
            return resp.status_code == 200
        except Exception as exc:
            logger.warning("OSINT server health check failed: %s", exc)
            return False

    async def search_username_filtered(
        self,
        username: str,
        verify_http: bool = False,
        min_confidence: float = 0.3,
        max_concurrency: int = 20,
        emit_kafka: bool = True,
    ) -> dict[str, Any]:
        """
        Call the full pipeline endpoint.

        Returns the raw JSON response dict.
        """
        client = await self._ensure_client()
        logger.info("search_username_filtered: user=%s", username)

        try:
            resp = await client.get(
                "/search_username_filtered",
                params={
                    "username": username,
                    "verify_http": verify_http,
                    "min_confidence": min_confidence,
                    "max_concurrency": max_concurrency,
                    "emit_kafka": emit_kafka,
                },
            )
            resp.raise_for_status()
            return resp.json()

        except httpx.HTTPStatusError as exc:
            logger.error(
                "OSINT server returned %d for user=%s: %s",
                exc.response.status_code, username, exc.response.text[:200],
            )
            raise OsintClientError(
                f"Server error {exc.response.status_code}: {exc.response.text[:200]}"
            ) from exc

        except httpx.RequestError as exc:
            logger.error("OSINT server request failed for user=%s: %s", username, exc)
            raise OsintClientError(f"Connection error: {exc}") from exc

    async def parse_url(self, url: str) -> dict[str, Any]:
        """Parse a profile URL into platform + username."""
        client = await self._ensure_client()
        try:
            resp = await client.get("/parse_url", params={"url": url})
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.error("parse_url failed for %s: %s", url, exc)
            raise OsintClientError(f"parse_url failed: {exc}") from exc

    async def search_username_raw(self, username: str) -> dict[str, Any]:
        """Call the raw search endpoint (no PPF scoring)."""
        client = await self._ensure_client()
        try:
            resp = await client.get("/search_username", params={"username": username})
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.error("search_username failed for %s: %s", username, exc)
            raise OsintClientError(f"search_username failed: {exc}") from exc
