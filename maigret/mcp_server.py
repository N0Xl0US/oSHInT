"""
mcp_server.py — MCP stdio tool server for the Maigret OSINT pipeline.

Exposes the same pipeline as the FastAPI app, but over MCP stdio transport
so an AI agent can call it as a tool.

Run with:
    python -m maigret.mcp_server
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from mcp.server.fastmcp import FastMCP

from maigret.config import get_settings
from maigret.events.event_builder import build_event
from maigret.kafka.producer import get_kafka_producer
from maigret.logging_config import setup_logging
from maigret.ppf import run_ppf
from maigret.runner import (
    MaigretError,
    normalize_platform,
    normalize_username,
    run_maigret,
)
from maigret.orchestrator.blackbird_source import BlackbirdSource
from maigret.orchestrator.spiderfoot_source import SpiderFootSource
from maigret.orchestrator import IdentityOrchestrator

# Initialise logging early for MCP mode
settings = get_settings()
setup_logging(level=settings.log_level, fmt=settings.log_format)

logger = logging.getLogger(__name__)
mcp = FastMCP("maigret-osint")


@mcp.tool()
def parse_url(url: str) -> dict:
    """Parse a profile URL into normalised platform and username fields."""
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    username = path[1:] if path.startswith("@") else path.split("/")[0]
    return {
        "url": url,
        "platform": normalize_platform(parsed.netloc),
        "extracted_username": normalize_username(username),
    }


@mcp.tool()
def search_username(username: str) -> dict:
    """
    Run maigret and return claimed profiles for a username (no PPF scoring).
    """
    import asyncio

    normalized = normalize_username(username)

    async def _run():
        return await run_maigret(normalized, settings=get_settings())

    result = asyncio.run(_run())
    return {
        "query": normalized,
        "total_scanned": result.total_scanned,
        "total_claimed": len(result.claimed_profiles),
        "profiles": [p.model_dump() for p in result.claimed_profiles],
    }


@mcp.tool()
async def search_username_filtered(
    username: str,
    verify_http: bool = False,
    min_confidence: float = 0.3,
    max_concurrency: int = 20,
    emit_kafka: bool = True,
    trigger: str = "api",
) -> dict:
    """
    Full OSINT pipeline tool:
    maigret → PPF → Event Builder → Kafka.

    Returns scored profiles with confidence, tier, and verification status.
    """
    cfg = get_settings()
    normalized = normalize_username(username)

    logger.info("MCP tool: search_username_filtered | user=%s", normalized)

    try:
        # Step 1: Maigret
        result = await run_maigret(normalized, settings=cfg)

        # Step 2: PPF
        scored = await run_ppf(
            result.claimed_profiles,
            verify_http=verify_http,
            max_concurrency=max_concurrency,
            min_confidence=min_confidence,
            http_timeout=cfg.ppf_http_timeout,
        )

        # Step 3: Event Builder
        payload_bytes, partition_key = build_event(
            username=normalized,
            profiles=scored,
            total_raw=result.total_scanned,
            total_claimed=len(result.claimed_profiles),
            trigger=trigger,
        )

        # Step 4: Kafka
        kafka_status = "skipped"
        publish_topic = cfg.kafka_probe_topic if trigger == "test" else cfg.kafka_topic
        if emit_kafka:
            try:
                kafka = get_kafka_producer(cfg)
                kafka.publish(payload_bytes, key=partition_key, topic=publish_topic)
                kafka_status = "published"
            except Exception as exc:
                kafka_status = f"error: {exc}"
                logger.error("Kafka publish failed in MCP tool: %s", exc)

        return {
            "query": normalized,
            "pipeline": {
                "total_raw": result.total_scanned,
                "total_claimed": len(result.claimed_profiles),
                "total_filtered": len(scored),
                "kafka_topic": publish_topic,
                "kafka_status": kafka_status,
            },
            "profiles": [p.model_dump() for p in scored],
        }

    except MaigretError as exc:
        logger.error("MCP tool error for user=%s: %s", normalized, exc)
        return {
            "error": str(exc),
            "query": normalized,
        }


@mcp.tool()
async def resolve_identity(
    username: str | None = None,
    name: str | None = None,
    email: str | None = None,
    linkedin_url: str | None = None,
    institution: str | None = None,
) -> dict:
    """
    Resolve identity using explicit-username or name/email-first source routing.

    Notes:
      - Username-only sources run only when username is explicitly provided.
            - Holehe and H8mail are used whenever email is available.
    """
    if not username and not name and not email and not linkedin_url:
        return {"error": "Provide at least one of: username, name, email, or linkedin_url"}

    orchestrator = IdentityOrchestrator()
    result = await orchestrator.resolve_identity(
        username=username,
        name=name,
        email=email,
        linkedin_url=linkedin_url,
        institution=institution,
    )
    return result.to_summary_dict()


@mcp.tool()
async def search_blackbird(
    username: str | None = None,
    email: str | None = None,
    export_pdf: bool = False,
    export_csv: bool = False,
    no_update: bool = True,
) -> dict:
    """
    Run Blackbird OSINT search for one username or one email.

    Returns discovered accounts and generated artifact paths when available.
    """
    source = BlackbirdSource()
    try:
        return await source.search(
            username=username,
            email=email,
            export_pdf=export_pdf,
            export_csv=export_csv,
            no_update=no_update,
        )
    except ValueError as exc:
        return {"error": str(exc)}
    except Exception as exc:
        logger.error("MCP tool error in search_blackbird: %s", exc)
        return {
            "error": str(exc),
            "query": username or email,
        }


@mcp.tool()
async def search_spiderfoot(
    email: str,
    min_confidence: float = 0.45,
) -> dict:
    """
    Run SpiderFoot email search and return mapped IdentityClaims.
    """
    normalized = (email or "").strip().lower()
    if not normalized:
        return {"error": "Provide a non-empty email"}

    source = SpiderFootSource()
    try:
        claims = await source.search(
            email=normalized,
            min_confidence=min_confidence,
        )
        return {
            "query": normalized,
            "total_claimed": len(claims),
            "claims": [claim.model_dump() for claim in claims],
        }
    except Exception as exc:
        logger.error("MCP tool error in search_spiderfoot: %s", exc)
        return {
            "error": str(exc),
            "query": normalized,
        }


if __name__ == "__main__":
    mcp.run(transport="stdio")