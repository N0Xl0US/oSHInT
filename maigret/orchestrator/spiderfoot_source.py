"""
orchestrator/spiderfoot_source.py - SpiderFoot identifier datasource adapter.

Runs SpiderFoot CLI for a username or email target and converts social profile events
into IdentityClaim objects for orchestrator and MCP usage.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from maigret.agent.report import IdentityClaim

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class SpiderFootSource:
    """Run SpiderFoot CLI and map social events to IdentityClaim rows."""

    def __init__(
        self,
        binary: str | None = None,
        request_timeout: int = 120,
        modules: str | None = None,
    ) -> None:
        configured_binary = (binary or os.getenv("SPIDERFOOT_BINARY") or "").strip()
        if configured_binary:
            self._binary = configured_binary
        else:
            docker_wrapper = Path(__file__).resolve().parents[2] / "scripts" / "spiderfoot-docker.sh"
            self._binary = str(docker_wrapper) if docker_wrapper.exists() else "sf.py"
        self._request_timeout = max(10, int(request_timeout))
        self._modules = (modules or os.getenv("SPIDERFOOT_MODULES") or "sfp_socialprofiles").strip()

    async def search(
        self,
        username: str | None = None,
        email: str | None = None,
        min_confidence: float = 0.45,
    ) -> list[IdentityClaim]:
        """Run SpiderFoot for one identifier and return discovered profile claims."""
        target_kind, target = _resolve_target(username=username, email=email)
        if not target:
            return []

        events = await self._run_spiderfoot(target)
        target_username = target if target_kind == "username" else target.split("@", 1)[0]
        target_email = target if target_kind == "email" else None
        claims = _events_to_claims(
            events=events,
            target_username=target_username,
            target_email=target_email,
            min_confidence=min_confidence,
        )
        logger.info("SpiderFootSource: %s=%s claims=%d", target_kind, target, len(claims))
        return claims

    async def _run_spiderfoot(self, username: str) -> list[dict[str, Any]]:
        proc = await self._spawn_spiderfoot_process(username=username)

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=self._request_timeout,
            )
        except asyncio.TimeoutError as exc:
            proc.kill()
            await proc.communicate()
            raise RuntimeError(
                f"SpiderFoot timed out after {self._request_timeout}s"
            ) from exc

        stdout_text = stdout.decode("utf-8", errors="ignore").strip()
        stderr_text = stderr.decode("utf-8", errors="ignore").strip()

        if proc.returncode != 0:
            details = stderr_text or stdout_text or "no output"
            raise RuntimeError(f"SpiderFoot failed with exit code {proc.returncode}: {details}")

        return _parse_events_output(stdout_text)

    async def _spawn_spiderfoot_process(self, username: str) -> asyncio.subprocess.Process:
        args = (
            "-s",
            username,
            "-m",
            self._modules,
            "-o",
            "json",
            "-n",
        )

        venv_script = str(Path(sys.prefix).resolve() / "bin" / "sf.py")
        sibling_script = str(Path(sys.executable).resolve().parent / "sf.py")

        try:
            return await asyncio.create_subprocess_exec(
                self._binary,
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            logger.warning(
                "SpiderFoot binary '%s' not found; retrying via '%s'",
                self._binary,
                venv_script,
            )
            try:
                return await asyncio.create_subprocess_exec(
                    venv_script,
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            except FileNotFoundError:
                try:
                    return await asyncio.create_subprocess_exec(
                        sibling_script,
                        *args,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                except FileNotFoundError as exc:
                    raise RuntimeError(
                        "SpiderFoot executable not found. Tried "
                        f"'{self._binary}', '{venv_script}', and '{sibling_script}'. "
                        "Install SpiderFoot or set SPIDERFOOT_BINARY."
                    ) from exc


def _normalise_username(value: str) -> str:
    candidate = (value or "").strip().lstrip("@").lower()
    return re.sub(r"\s+", "", candidate)


def _normalise_email(value: str) -> str:
    return (value or "").strip().lower()


def _resolve_target(username: str | None, email: str | None) -> tuple[str, str]:
    user = _normalise_username(username or "")
    mail = _normalise_email(email or "")

    if user and mail:
        raise ValueError("Provide either username or email, not both")
    if user:
        return "username", user
    if mail:
        if not _EMAIL_RE.match(mail):
            raise ValueError("Email must be a valid address")
        return "email", mail
    return "", ""


def _parse_events_output(raw: str) -> list[dict[str, Any]]:
    if not raw:
        return []

    parsed: Any
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = _extract_json_fragment(raw)

    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]

    if isinstance(parsed, dict):
        for key in ("events", "results", "data", "records"):
            value = parsed.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [parsed]

    return []


def _extract_json_fragment(raw: str) -> Any:
    start = -1
    end = -1

    for i, ch in enumerate(raw):
        if ch in "[{":
            start = i
            break

    for i in range(len(raw) - 1, -1, -1):
        if raw[i] in "]}":
            end = i
            break

    if start != -1 and end > start:
        snippet = raw[start:end + 1]
        try:
            return json.loads(snippet)
        except json.JSONDecodeError:
            return []

    return []


def _events_to_claims(
    events: list[dict[str, Any]],
    target_username: str,
    target_email: str | None,
    min_confidence: float,
) -> list[IdentityClaim]:
    claims: list[IdentityClaim] = []
    seen: set[tuple[str, str, str | None]] = set()

    for event in events:
        url = _extract_url(event)
        if not url:
            continue

        platform = _extract_platform(url)
        resolved_username = _extract_username(url) or target_username
        confidence = _score_event(event, target_username, resolved_username, url)
        if confidence < float(min_confidence):
            continue

        dedup_key = (platform, resolved_username.lower(), url)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        claims.append(
            IdentityClaim(
                platform=platform,
                url=url,
                username=resolved_username.lower(),
                email=target_email,
                confidence=round(confidence, 3),
                tier=_to_tier(confidence),
                verified=True,
                source_tool="spiderfoot",
            )
        )

    return claims


def _extract_url(event: dict[str, Any]) -> str | None:
    for key in ("url", "data", "value", "source_data"):
        value = event.get(key)
        if isinstance(value, str) and value.strip().startswith("http"):
            return value.strip()
    return None


def _extract_platform(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]

    if host.endswith("x.com"):
        return "x"

    parts = [part for part in host.split(".") if part]
    if len(parts) >= 2:
        return parts[-2]
    return host or "spiderfoot"


def _extract_username(url: str) -> str | None:
    parsed = urlparse(url)
    segments = [seg for seg in parsed.path.split("/") if seg]
    if not segments:
        return None

    host = (parsed.netloc or "").lower()
    if "linkedin.com" in host and len(segments) >= 2 and segments[0].lower() in {"in", "pub"}:
        return segments[1].lstrip("@").strip().lower() or None

    return segments[0].lstrip("@").strip().lower() or None


def _score_event(
    event: dict[str, Any],
    target_username: str,
    resolved_username: str,
    url: str,
) -> float:
    score = 0.58

    event_type = str(event.get("type") or event.get("event_type") or "").strip().upper()
    if "SOCIAL" in event_type or "PROFILE" in event_type:
        score += 0.12

    if resolved_username and resolved_username.lower() == target_username.lower():
        score += 0.10

    if url.startswith("https://"):
        score += 0.05

    source = str(event.get("source") or "").strip().lower()
    if "spiderfoot" in source:
        score += 0.03

    return min(score, 0.92)


def _to_tier(confidence: float) -> str:
    if confidence >= 0.75:
        return "high"
    if confidence >= 0.45:
        return "medium"
    return "low"
