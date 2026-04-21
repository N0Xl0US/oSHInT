"""
main.py — Production FastAPI application for the Maigret OSINT pipeline.

Features:
  - Lifespan handler (replaces deprecated @app.on_event)
  - Request-ID middleware
  - /health and /ready endpoints
  - Proper error handlers for pipeline exceptions
  - Full pipeline: maigret → PPF → event builder → Kafka
"""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse
from typing import Any

import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from maigret.config import get_settings
from maigret.events.event_builder import build_event
from maigret.kafka.producer import get_kafka_producer
from maigret.logging_config import setup_logging
from maigret.academic.models import AcademicSearchResult
from maigret.academic.runner import AcademicSearchTool
from maigret.academic.event_builder import build_academic_event
from maigret.models import (
    HealthResponse,
    PipelineMeta,
    PipelineResponse,
    ParseUrlResponse,
    ReadyResponse,
    SearchResponse,
)
from maigret.ppf import run_ppf
from maigret.runner import (
    MaigretError,
    MaigretNotFoundError,
    MaigretTimeoutError,
    normalize_platform,
    normalize_username,
    run_maigret,
)
from maigret.orchestrator.github_playwright_source import GitHubPlaywrightSource
from maigret.intake import ResumeParseError, parse_resume_bytes

logger = logging.getLogger(__name__)

_FRONTEND_INDEX = Path(__file__).resolve().parent.parent / "frontend" / "index.html"

_NAME_TOOL_SUGGESTIONS: list[dict[str, str]] = [
    {
        "tool": "search_academic",
        "reason": "Best name-first source for person entities (OpenAlex + Semantic Scholar + Orchid when configured).",
    },
    {
        "tool": "search_spiderfoot",
        "reason": "Use known email addresses to discover additional profile links via SpiderFoot social modules.",
    },
    {
        "tool": "search_github",
        "reason": "Try a handle derived from the person's name for quick account validation.",
    },
    {
        "tool": "resolve_identity",
        "reason": "Use coordinated multi-source identity resolution for combined inputs.",
    },
]

_GITHUB_RESERVED_PATHS = {
    "about",
    "apps",
    "codespaces",
    "collections",
    "contact",
    "dashboard",
    "enterprise",
    "events",
    "explore",
    "features",
    "issues",
    "join",
    "login",
    "marketplace",
    "new",
    "notifications",
    "organizations",
    "orgs",
    "pricing",
    "pulls",
    "search",
    "settings",
    "sponsors",
    "support",
    "topics",
    "trending",
}


# ── Lifespan ─────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown hook — manages Kafka producer lifecycle."""
    settings = get_settings()
    setup_logging(level=settings.log_level, fmt=settings.log_format)
    logger.info("Maigret MCP Server starting (v2.0)")

    kafka = get_kafka_producer(settings)
    yield

    logger.info("Shutdown: flushing Kafka producer…")
    kafka.close()
    logger.info("Kafka flush complete. Goodbye.")


app = FastAPI(
    title="Maigret MCP Server",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Middleware ────────────────────────────────────────────────────────────────


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Attach a unique request ID to every request for log correlation."""
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    request.state.request_id = request_id

    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# ── Exception handlers ───────────────────────────────────────────────────────


@app.exception_handler(MaigretNotFoundError)
async def _maigret_not_found(request: Request, exc: MaigretNotFoundError):
    return JSONResponse(status_code=500, content={"error": str(exc)})


@app.exception_handler(MaigretTimeoutError)
async def _maigret_timeout(request: Request, exc: MaigretTimeoutError):
    return JSONResponse(status_code=504, content={"error": str(exc)})


@app.exception_handler(MaigretError)
async def _maigret_error(request: Request, exc: MaigretError):
    return JSONResponse(status_code=502, content={"error": str(exc)})


# ── Health endpoints ─────────────────────────────────────────────────────────


@app.get("/health", response_model=HealthResponse)
def health():
    """Liveness probe — always 200 if the process is running."""
    return HealthResponse(status="ok")


@app.get("/ready", response_model=ReadyResponse)
def ready():
    """Readiness probe — checks Kafka broker connectivity."""
    kafka = get_kafka_producer()
    connected = kafka.health_check(timeout=5.0)
    if not connected:
        raise HTTPException(status_code=503, detail="Kafka broker unreachable")
    return ReadyResponse(kafka="connected")


# ── Endpoints ────────────────────────────────────────────────────────────────


@app.get("/", include_in_schema=False)
def frontend_home():
    """Serve the functional frontend UI."""
    if _FRONTEND_INDEX.exists():
        return FileResponse(_FRONTEND_INDEX)
    return JSONResponse(
        status_code=404,
        content={"error": f"Frontend not found at {_FRONTEND_INDEX}"},
    )


@app.get("/parse_url", response_model=ParseUrlResponse)
def parse_url(url: str):
    """Parse a profile URL into normalised platform and username."""
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    username = path[1:] if path.startswith("@") else path.split("/")[0]
    return ParseUrlResponse(
        url=url,
        platform=normalize_platform(parsed.netloc),
        extracted_username=normalize_username(username),
    )


def _extract_github_username_from_links(links: list[Any]) -> str | None:
    """Extract a GitHub username from normalized resume links."""
    profile_urls = _extract_github_profile_urls_from_links(links)
    if not profile_urls:
        return None
    path = urlparse(profile_urls[0]).path.strip("/")
    return normalize_username(path) if path else None


def _extract_github_profile_urls_from_links(links: list[Any]) -> list[str]:
    """Extract all unique canonical GitHub profile URLs from normalized resume links."""
    profile_urls: list[str] = []
    seen: set[str] = set()

    for link in links:
        platform = str(getattr(link, "platform", "") or "").lower()
        if platform != "github":
            continue

        url = str(getattr(link, "url", "") or "").strip()
        if not url:
            continue

        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if not host.endswith("github.com"):
            continue

        path = parsed.path.strip("/")
        if not path:
            continue

        candidate = path.split("/", 1)[0].lstrip("@").strip()
        if not candidate:
            continue
        if candidate.lower() in _GITHUB_RESERVED_PATHS:
            continue

        normalized = normalize_username(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        profile_urls.append(f"https://github.com/{normalized}")

    return profile_urls


def _extract_linkedin_url_from_links(links: list[Any]) -> str | None:
    """Extract first normalized LinkedIn profile URL from resume links."""
    for link in links:
        platform = str(getattr(link, "platform", "") or "").lower()
        if platform != "linkedin":
            continue

        url = str(getattr(link, "url", "") or "").strip()
        if not url:
            continue

        try:
            from maigret.orchestrator.linkedin_ppf import normalize_linkedin_profile_url

            normalized = normalize_linkedin_profile_url(url)
            if normalized:
                return normalized
        except Exception:
            return url

    return None


@app.get("/search_username", response_model=SearchResponse)
async def search_username(username: str):
    """Run maigret and return raw claimed profiles (no PPF scoring)."""
    settings = get_settings()
    normalized = normalize_username(username)

    result = await run_maigret(normalized, settings=settings)

    return SearchResponse(
        query=normalized,
        total_scanned=result.total_scanned,
        total_claimed=len(result.claimed_profiles),
        profiles=result.claimed_profiles,
    )


@app.get("/search_username_filtered", response_model=PipelineResponse)
async def search_username_filtered(
    username: str,
    verify_http: bool = False,
    min_confidence: float = 0.3,
    max_concurrency: int = 20,
    emit_kafka: bool = True,
    trigger: str = "api",
):
    """
    Full pipeline:
      1. maigret   → raw NDJSON rows
      2. PPF       → confidence-scored, HTTP-verified profiles
      3. EventBuilder → canonical osint.raw.maigret.v1 payload
      4. Kafka     → publish to osint.raw.maigret.v1
    """
    settings = get_settings()
    normalized = normalize_username(username)
    logger.info(
        "▶ Pipeline start | user=%s verify_http=%s min_conf=%.2f",
        normalized, verify_http, min_confidence,
    )

    # Step 1: Maigret
    result = await run_maigret(normalized, settings=settings)
    logger.info(
        "  Maigret: scanned=%d claimed=%d",
        result.total_scanned, len(result.claimed_profiles),
    )

    # Step 2: PPF
    scored = await run_ppf(
        result.claimed_profiles,
        verify_http=verify_http,
        max_concurrency=max_concurrency,
        min_confidence=min_confidence,
        http_timeout=settings.ppf_http_timeout,
    )
    logger.info("  PPF: filtered=%d", len(scored))

    # Step 3: Event Builder
    payload_bytes, partition_key = build_event(
        username=normalized,
        profiles=scored,
        total_raw=result.total_scanned,
        total_claimed=len(result.claimed_profiles),
        trigger=trigger,
    )

    # Step 4: Kafka Publish
    kafka_status = "skipped"
    publish_topic = settings.kafka_probe_topic if trigger == "test" else settings.kafka_topic
    if emit_kafka:
        try:
            kafka = get_kafka_producer()
            kafka.publish(payload_bytes, key=partition_key, topic=publish_topic)
            kafka_status = "published"
            logger.info(
                "  Kafka: published | topic=%s key=%s bytes=%d",
                publish_topic,
                partition_key,
                len(payload_bytes),
            )
        except Exception as exc:
            logger.error("  Kafka publish FAILED: %s", exc)
            kafka_status = f"error: {exc}"

    return PipelineResponse(
        query=normalized,
        pipeline=PipelineMeta(
            total_raw=result.total_scanned,
            total_claimed=len(result.claimed_profiles),
            total_filtered=len(scored),
            kafka_topic=publish_topic,
            kafka_status=kafka_status,
        ),
        profiles=scored,
    )


@app.get("/search_academic", response_model=AcademicSearchResult)
async def search_academic(
    name: str,
    institution: str | None = None,
    min_confidence: float = 0.3,
    emit_kafka: bool = True,
):
    """
    Search for academic publications authored by a person.

    Queries Semantic Scholar and OpenAlex, optionally adds Orchid when
    credentials are configured, scores and merges results, and optionally
    publishes to the osint.raw.academic.v1 Kafka topic.
    """
    logger.info(
        "▶ Academic search | name=%s institution=%s min_conf=%.2f",
        name, institution, min_confidence,
    )

    tool = AcademicSearchTool()
    result = await tool.search_academic(
        name=name,
        institution=institution,
        min_confidence=min_confidence,
    )

    # Kafka publish
    if emit_kafka and result.profiles:
        try:
            payload_bytes, key = build_academic_event(
                query_name=name,
                profiles=result.profiles,
                query_institution=institution,
                trigger="api",
            )
            kafka = get_kafka_producer()
            kafka.publish(payload_bytes, key=key, topic="osint.raw.academic.v1")
            logger.info(
                "  Kafka: published academic event | key=%s bytes=%d",
                key, len(payload_bytes),
            )
        except Exception as exc:
            logger.error("  Kafka publish FAILED for academic event: %s", exc)

    return result


@app.get("/search_github")
async def search_github(
    username: str,
    min_confidence: float = 0.35,
):
    """Run combined GitHub datasource lookup (Octosuite + Playwright)."""
    claims = []
    errors: list[str] = []

    playwright_source = GitHubPlaywrightSource()
    try:
        claims.extend(
            await playwright_source.search(username=username, min_confidence=min_confidence)
        )
    except Exception as exc:
        errors.append(f"github_playwright: {exc}")

    try:
        from maigret.orchestrator.github_octosuite_ppf import (
            GitHubOctosuitePPF,
            SubjectQuery as OctosuiteSubjectQuery,
        )

        octo_adapter = GitHubOctosuitePPF()
        octo_claims = await octo_adapter.run(
            OctosuiteSubjectQuery(username=username, username_hint="github")
        )
        claims.extend([claim for claim in octo_claims if claim.confidence >= min_confidence])
    except Exception as exc:
        errors.append(f"github_octosuite: {exc}")

    from maigret.orchestrator.orchestrator import IdentityOrchestrator

    claims = IdentityOrchestrator()._reconcile_github_claims(claims)

    return {
        "query": username,
        "source": "github_fusion",
        "total_claims": len(claims),
        "errors": errors,
        "profiles": [
            {
                "platform": c.platform,
                "url": c.url,
                "username": c.username,
                "confidence": c.confidence,
                "tier": c.tier,
                "verified": c.verified,
                "source_tool": c.source_tool,
                "email": c.email,
                "institutions": c.institutions,
                "research_domains": c.research_domains,
                "raw_data": c.raw_data,
            }
            for c in claims
        ],
    }


@app.get("/resolve_identity")
async def resolve_identity(
    username: str | None = None,
    name: str | None = None,
    email: str | None = None,
    linkedin_url: str | None = None,
    institution: str | None = None,
):
    """
    Trigger full identity resolution across Maigret, Academic, GitHub, Holehe, and related sources.

    Accepts either:
        - username only
        - name only
        - email only
        - both username and name
    """
    if not username and not name and not email and not linkedin_url:
        raise HTTPException(
            status_code=422,
            detail="Provide at least one of: username, name, email, or linkedin_url",
        )

    from maigret.orchestrator import IdentityOrchestrator

    orchestrator = IdentityOrchestrator()
    result = await orchestrator.resolve_identity(
        username=username,
        name=name,
        email=email,
        linkedin_url=linkedin_url,
        institution=institution,
    )
    return result.to_summary_dict()


@app.post("/resolve_identity_intake")
async def resolve_identity_intake(
    resume: UploadFile | None = File(default=None),
    name: str | None = Form(default=None),
    username: str | None = Form(default=None),
    email: str | None = Form(default=None),
    linkedin_url: str | None = Form(default=None),
    institution: str | None = Form(default=None),
):
    """
    Resume-first identity intake endpoint.

    Behavior:
    - If a resume is uploaded, extract name/email/phone/username hints and use them.
    - Username-only sources are invoked when username is explicit or promoted from resume GitHub links/usernames.
    - If no resume is uploaded, auto-run name/email-capable sources and return tool suggestions.
    """
    extracted: dict[str, Any] | None = None
    resume_github_profile_urls: list[str] = []
    provided_email = (email or "").strip().lower() or None
    candidate_emails: list[str] = [provided_email] if provided_email else []

    if resume is not None and (resume.filename or "").strip():
        raw_bytes = await resume.read()
        try:
            parsed = parse_resume_bytes(
                content=raw_bytes,
                filename=resume.filename,
                content_type=resume.content_type,
            )
        except ResumeParseError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        extracted = {
            "filename": resume.filename,
            "name": parsed.name,
            "emails": parsed.emails,
            "phones": parsed.phones,
            "usernames": parsed.usernames,
            "institutions": parsed.institutions,
            "links": [
                {
                    "url": link.url,
                    "platform": link.platform,
                    "source": link.source,
                }
                for link in parsed.links
            ],
            "text_length": parsed.text_length,
        }

        name = name or parsed.name
        resume_github_profile_urls = _extract_github_profile_urls_from_links(parsed.links)
        if not username:
            if resume_github_profile_urls:
                username = _extract_github_username_from_links(parsed.links)
            elif parsed.usernames:
                username = normalize_username(parsed.usernames[0])
        linkedin_url = linkedin_url or _extract_linkedin_url_from_links(parsed.links)
        for parsed_email in parsed.emails:
            value = (parsed_email or "").strip().lower()
            if "@" in value:
                candidate_emails.append(value)

        email = email or (parsed.emails[0] if parsed.emails else None)
        institution = institution or (parsed.institutions[0] if parsed.institutions else None)

    # Keep email routing deterministic while still searching all discovered emails.
    deduped_emails: list[str] = []
    seen_emails: set[str] = set()
    for value in candidate_emails:
        normalized = (value or "").strip().lower()
        if "@" not in normalized or normalized in seen_emails:
            continue
        seen_emails.add(normalized)
        deduped_emails.append(normalized)

    if not email and deduped_emails:
        email = deduped_emails[0]

    github_profile_urls: list[str] = []
    for raw_url in resume_github_profile_urls:
        normalized_url = raw_url.strip()
        if not normalized_url or normalized_url in github_profile_urls:
            continue
        github_profile_urls.append(normalized_url)

    if not github_profile_urls and username:
        normalized_username = normalize_username(username)
        if normalized_username:
            github_profile_urls.append(f"https://github.com/{normalized_username}")

    github_usernames: list[str] = []
    for profile_url in github_profile_urls:
        path = urlparse(profile_url).path.strip("/")
        normalized = normalize_username(path)
        if not normalized or normalized in github_usernames:
            continue
        github_usernames.append(normalized)

    if not username and not name and not email and not linkedin_url:
        raise HTTPException(
            status_code=422,
            detail="Provide at least one of: resume, username, name, email, or linkedin_url",
        )

    from maigret.orchestrator import IdentityOrchestrator

    orchestrator = IdentityOrchestrator()
    result = await orchestrator.resolve_identity(
        username=username,
        github_profile_urls=github_profile_urls if github_profile_urls else None,
        name=name,
        email=email,
        emails=deduped_emails if deduped_emails else None,
        linkedin_url=linkedin_url,
        institution=institution,
    )
    summary = result.to_summary_dict()

    if extracted is not None:
        mode = "resume"
    elif name and email and not username:
        mode = "name_email"
    elif name and not username:
        mode = "name_only"
    elif email and not username:
        mode = "email_only"
    else:
        mode = "direct"
    summary["intake"] = {
        "mode": mode,
        "resume_uploaded": extracted is not None,
        "resolved_inputs": {
            "name": name,
            "username": username,
            "github_profile_urls": github_profile_urls,
            "github_usernames": github_usernames,
            "email": email,
            "emails": deduped_emails,
            "linkedin_url": linkedin_url,
            "institution": institution,
        },
        "extracted": extracted,
    }

    if extracted is None:
        summary["tool_suggestions"] = _NAME_TOOL_SUGGESTIONS

    return summary


# ── Entry point ──────────────────────────────────────────────────────────────


def run() -> None:
    """CLI entry point for ``maigret-server``."""
    settings = get_settings()
    uvicorn.run(
        "maigret.main:app",
        host=settings.server_host,
        port=settings.server_port,
        reload=False,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    run()
