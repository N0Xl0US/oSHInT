"""
orchestrator/holehe_source.py — Holehe email enumeration datasource adapter.

Runs the Holehe CLI for an email and converts positive site hits into
IdentityClaim objects for the orchestrator.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import re
import sys
import tempfile
from pathlib import Path

from maigret.agent.report import IdentityClaim

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_USED_SITE_RE = re.compile(r"^\[\+\]\s+(.+)$")


class HoleheSource:
    """Run Holehe for email discovery and convert results to IdentityClaim."""

    def __init__(
        self,
        binary: str | None = None,
        request_timeout: int = 30,
        per_site_timeout: int = 10,
    ) -> None:
        configured_binary = (binary or os.getenv("HOLEHE_BINARY") or "").strip()
        if configured_binary:
            self._binary = configured_binary
        else:
            venv_script = Path(sys.prefix).resolve() / "bin" / "holehe"
            self._binary = str(venv_script) if venv_script.exists() else "holehe"
        self._request_timeout = max(5, int(request_timeout))
        self._per_site_timeout = max(1, int(per_site_timeout))

    async def search(self, email: str, min_confidence: float = 0.45) -> list[IdentityClaim]:
        """Run Holehe and return one claim per site where the email appears used."""
        target = email.strip().lower()
        if not _looks_like_email(target):
            return []

        rows = await self._run_holehe(target)
        if not rows:
            logger.info("HoleheSource: email=%s returned no rows", target)
            return []

        claims: list[IdentityClaim] = []
        default_username = target.split("@", 1)[0]
        for row in rows:
            if not _is_truthy(row.get("exists", "")):
                continue

            domain = (row.get("domain") or "").strip().lower()
            platform = (row.get("name") or domain or "holehe").strip().lower()
            confidence = _score_row(row)
            if confidence < float(min_confidence):
                continue

            claims.append(
                IdentityClaim(
                    platform=platform,
                    url=f"https://{domain}" if domain else None,
                    username=default_username,
                    email=target,
                    confidence=round(confidence, 3),
                    tier=_to_tier(confidence),
                    verified=True,
                    source_tool="holehe",
                )
            )

        logger.info("HoleheSource: email=%s claims=%d", target, len(claims))
        return claims

    async def _run_holehe(self, email: str) -> list[dict[str, str]]:
        with tempfile.TemporaryDirectory(prefix="holehe_") as tmpdir:
            proc = await self._spawn_holehe_process(email=email, cwd=tmpdir)

            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=self._request_timeout,
                )
            except asyncio.TimeoutError as exc:
                proc.kill()
                raise RuntimeError(
                    f"Holehe timed out after {self._request_timeout}s"
                ) from exc

            stdout_text = stdout.decode("utf-8", errors="ignore")
            stderr_text = stderr.decode("utf-8", errors="ignore")

            csv_files = sorted(Path(tmpdir).glob("holehe_*_results.csv"))
            rows: list[dict[str, str]] = []
            for csv_file in csv_files:
                with csv_file.open("r", encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle)
                    rows.extend({k: (v or "") for k, v in row.items()} for row in reader)

            # holehe <email> --only-used prints positives to stdout as "[+] domain".
            if not rows:
                rows = _rows_from_only_used_output(stdout_text)

            if proc.returncode != 0:
                stderr_text = stderr_text.strip()
                stdout_text = stdout_text.strip()
                if rows:
                    logger.warning(
                        "Holehe exited with code %s but produced %d parsed rows; accepting output",
                        proc.returncode,
                        len(rows),
                    )
                    return rows
                details = stderr_text or stdout_text or "no output"
                raise RuntimeError(f"Holehe failed with exit code {proc.returncode}: {details}")

            return rows

    async def _spawn_holehe_process(self, email: str, cwd: str) -> asyncio.subprocess.Process:
        args = (email, "--only-used")
        venv_script = str(Path(sys.prefix).resolve() / "bin" / "holehe")
        sibling_script = str(Path(sys.executable).resolve().parent / "holehe")

        try:
            return await asyncio.create_subprocess_exec(
                self._binary,
                *args,
                cwd=cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            logger.warning(
                "Holehe binary '%s' not found; retrying via '%s'",
                self._binary,
                venv_script,
            )
            try:
                return await asyncio.create_subprocess_exec(
                    venv_script,
                    *args,
                    cwd=cwd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            except FileNotFoundError:
                try:
                    return await asyncio.create_subprocess_exec(
                        sibling_script,
                        *args,
                        cwd=cwd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                except FileNotFoundError as fallback_exc:
                    raise RuntimeError(
                        "Holehe executable not found. Tried "
                        f"'{self._binary}', '{venv_script}', and '{sibling_script}'. "
                        "Install holehe in the active environment or set HOLEHE_BINARY."
                    ) from fallback_exc


def _rows_from_only_used_output(output: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen_domains: set[str] = set()

    for raw_line in output.splitlines():
        match = _USED_SITE_RE.match(raw_line.strip())
        if not match:
            continue

        domain = match.group(1).strip().lower()
        if " " in domain or "." not in domain:
            continue
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)

        rows.append(
            {
                "name": domain,
                "domain": domain,
                "method": "",
                "rateLimit": "",
                "exists": "True",
                "emailrecovery": "",
                "phoneNumber": "",
            }
        )

    return rows


def _looks_like_email(value: str) -> bool:
    return bool(_EMAIL_RE.match(value or ""))


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _score_row(row: dict[str, str]) -> float:
    score = 0.56

    if row.get("method", "").strip():
        score += 0.08
    if row.get("emailrecovery", "").strip():
        score += 0.06
    if row.get("phoneNumber", "").strip():
        score += 0.06
    if row.get("rateLimit", "").strip().lower() in {"0", "false", "no"}:
        score += 0.10

    return min(score, 0.90)


def _to_tier(confidence: float) -> str:
    if confidence >= 0.75:
        return "high"
    if confidence >= 0.45:
        return "medium"
    return "low"
