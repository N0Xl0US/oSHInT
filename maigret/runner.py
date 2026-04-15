"""
runner.py — Async maigret subprocess runner.

Never blocks the event loop: uses ``asyncio.create_subprocess_exec``.
Returns structured ``MaigretResult`` instead of raw lists.
"""

from __future__ import annotations

import asyncio
import glob
import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from maigret.config import Settings, get_settings
from maigret.models import RawProfile

logger = logging.getLogger(__name__)


# ── Exceptions ──────────────────────────────────────────────────────────────


class MaigretError(Exception):
    """Base exception for maigret subprocess failures."""


class MaigretNotFoundError(MaigretError):
    """Raised when the maigret binary is not on PATH."""


class MaigretTimeoutError(MaigretError):
    """Raised when maigret exceeds the wall-clock timeout."""


class MaigretExecutionError(MaigretError):
    """Raised when maigret exits with a non-zero return code."""


class MaigretParseError(MaigretError):
    """Raised when maigret produces no usable output."""


# ── Result model ─────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class MaigretResult:
    """Structured output of a maigret run."""

    rows: list[dict] = field(default_factory=list)
    total_scanned: int = 0
    claimed_profiles: list[RawProfile] = field(default_factory=list)


# ── Username normalisation ───────────────────────────────────────────────────

import re

_USERNAME_RE = re.compile(r"[^a-z0-9_]")


def normalize_username(username: str) -> str:
    """Lowercase, strip, remove non-alphanumeric (except underscore)."""
    return _USERNAME_RE.sub("", username.lower().strip())


def normalize_platform(site: str) -> str:
    """Take up to the first space (e.g. 'Steam (Group)' → 'steam')."""
    return site.lower().split(" ")[0] if site else "unknown"


# ── Profile extraction ───────────────────────────────────────────────────────


def extract_profiles(rows: list[dict], username: str) -> list[RawProfile]:
    """
    Parse maigret NDJSON rows into ``RawProfile`` models.

    Handles both dict-style (legacy) and list-style (NDJSON) output.
    """
    profiles: list[RawProfile] = []

    for row in rows:
        status_info = row.get("status") or {}
        if status_info.get("status") != "Claimed":
            continue

        site_info = row.get("site") or {}
        platform = normalize_platform(
            row.get("sitename") or status_info.get("site_name") or "unknown"
        )
        url = row.get("url_user") or status_info.get("url")
        check_type = (site_info.get("checkType") or "unknown").strip().lower()

        try:
            source_http_status = int(row.get("http_status") or 0)
        except (TypeError, ValueError):
            source_http_status = 0

        ids = status_info.get("ids") or {}

        profiles.append(RawProfile(
            platform=platform,
            url=url,
            username=username,
            check_type=check_type,
            source_http_status=source_http_status,
            has_ids=bool(ids),
            is_similar=bool(row.get("is_similar")),
        ))

    return profiles


# ── Async subprocess runner ──────────────────────────────────────────────────


async def run_maigret(
    username: str,
    settings: Settings | None = None,
) -> MaigretResult:
    """
    Run maigret as an async subprocess, parse NDJSON output.

    Raises:
        MaigretNotFoundError: maigret binary missing.
        MaigretTimeoutError:  exceeded wall-clock limit.
        MaigretExecutionError: non-zero exit code.
        MaigretParseError:    no usable NDJSON output produced.
    """
    settings = settings or get_settings()
    normalized = normalize_username(username)
    binary = _resolve_maigret_binary(settings.maigret_binary)

    with tempfile.TemporaryDirectory() as output_dir:
        cmd = [
            binary,
            normalized,
            "-J", "ndjson",
            "--folderoutput", output_dir,
            "--no-color",
            "--no-progressbar",
            "--timeout", str(settings.maigret_timeout),
            "--retries", str(settings.maigret_retries),
        ]

        logger.info(
            "Spawning maigret subprocess",
            extra={"username": normalized, "step": "maigret_start"},
        )

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise MaigretNotFoundError(
                f"maigret binary not found at '{binary}'. "
                "Install with: pip install maigret"
            ) from exc

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=settings.maigret_subprocess_timeout,
            )
        except asyncio.TimeoutError as exc:
            proc.kill()
            await proc.wait()
            raise MaigretTimeoutError(
                f"maigret exceeded {settings.maigret_subprocess_timeout}s wall-clock timeout "
                f"for username '{normalized}'"
            ) from exc

        if proc.returncode != 0:
            stderr_text = (stderr or b"").decode(errors="replace").strip()
            raise MaigretExecutionError(
                f"maigret exited {proc.returncode} for '{normalized}': "
                f"{stderr_text or 'no stderr output'}"
            )

        # Parse NDJSON report files
        report_files = sorted(glob.glob(os.path.join(output_dir, "report_*_ndjson.json")))
        if not report_files:
            raise MaigretParseError(
                f"maigret did not produce any NDJSON report files for '{normalized}'"
            )

        rows: list[dict] = []
        parse_errors = 0
        for report_file in report_files:
            with open(report_file, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        parse_errors += 1
                        if parse_errors <= 5:
                            logger.warning(
                                "Skipping invalid NDJSON line %d in %s: %s",
                                line_no, os.path.basename(report_file), line[:120],
                            )

        if not rows:
            raise MaigretParseError(
                f"maigret produced empty NDJSON for '{normalized}' "
                f"({len(report_files)} files, {parse_errors} parse errors)"
            )

        if parse_errors:
            logger.warning(
                "NDJSON parse completed with %d errors across %d files",
                parse_errors, len(report_files),
            )

        claimed = extract_profiles(rows, normalized)

        logger.info(
            "Maigret complete: scanned=%d claimed=%d",
            len(rows), len(claimed),
            extra={"username": normalized, "step": "maigret_done"},
        )

        return MaigretResult(
            rows=rows,
            total_scanned=len(rows),
            claimed_profiles=claimed,
        )


def _resolve_maigret_binary(configured_binary: str) -> str:
    """Resolve configured maigret binary, falling back to local project venv."""
    if os.path.sep in configured_binary:
        return configured_binary

    if shutil.which(configured_binary):
        return configured_binary

    project_venv_binary = Path(__file__).resolve().parent.parent / "venv" / "bin" / "maigret"
    if project_venv_binary.exists():
        return str(project_venv_binary)

    return configured_binary
