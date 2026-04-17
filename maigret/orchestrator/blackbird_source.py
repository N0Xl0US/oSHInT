"""
orchestrator/blackbird_source.py - Blackbird datasource adapter for MCP usage.

Runs the Blackbird CLI for a username or email target and returns structured
results for MCP clients, including discovered accounts and generated artifacts.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from maigret.agent.report import IdentityClaim

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class BlackbirdSource:
    """Run Blackbird CLI and return structured account discovery results."""

    def __init__(
        self,
        blackbird_root: str | Path | None = None,
        binary: str | None = None,
        request_timeout: int = 240,
    ) -> None:
        default_root = Path(__file__).resolve().parents[2] / "blackbird"
        configured_root = blackbird_root or os.getenv("BLACKBIRD_ROOT") or default_root
        self._root = Path(configured_root).resolve()
        self._script_path = self._root / "blackbird.py"
        self._binary = (binary or os.getenv("BLACKBIRD_BINARY") or sys.executable).strip()
        self._request_timeout = max(10, int(request_timeout))

    async def search(
        self,
        username: str | None = None,
        email: str | None = None,
        export_pdf: bool = False,
        export_csv: bool = False,
        no_update: bool = True,
    ) -> dict[str, Any]:
        """Run Blackbird and return accounts plus generated artifact paths."""
        query_type, query_value = _resolve_query(username=username, email=email)
        _ensure_query_valid(query_type=query_type, query_value=query_value)
        self._ensure_runtime_paths()

        command = self._build_command(
            query_type=query_type,
            query_value=query_value,
            export_pdf=export_pdf,
            export_csv=export_csv,
            no_update=no_update,
        )

        logger.info("BlackbirdSource: running blackbird for %s=%s", query_type, query_value)
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(self._root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self._request_timeout,
            )
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.communicate()
            raise RuntimeError(
                f"Blackbird timed out after {self._request_timeout}s"
            ) from exc

        stdout_text = _tail_output(stdout)
        stderr_text = _tail_output(stderr)

        if process.returncode != 0:
            details = stderr_text or stdout_text or "no output"
            raise RuntimeError(
                f"Blackbird failed with exit code {process.returncode}: {details}"
            )

        artifact_paths = self._resolve_artifact_paths(
            query_value=query_value,
            export_pdf=export_pdf,
            export_csv=export_csv,
        )
        accounts = _read_accounts_json(artifact_paths.get("json"))

        return {
            "query": query_value,
            "query_type": query_type,
            "total_found": len(accounts),
            "accounts": accounts,
            "artifacts": artifact_paths,
            "runner": {
                "command": command,
                "stdout_tail": stdout_text,
                "stderr_tail": stderr_text,
            },
        }

    async def search_claims(
        self,
        username: str | None = None,
        email: str | None = None,
        min_confidence: float = 0.45,
    ) -> list[IdentityClaim]:
        """Run Blackbird and map discovered accounts to IdentityClaim rows."""
        result = await self.search(
            username=username,
            email=email,
            export_pdf=False,
            export_csv=False,
            no_update=True,
        )

        query = str(result.get("query") or "").strip()
        query_type = str(result.get("query_type") or "").strip().lower()
        if not query:
            return []

        default_username = query if query_type == "username" else query.split("@", 1)[0]
        default_email = query if query_type == "email" else None

        claims: list[IdentityClaim] = []
        for account in result.get("accounts", []):
            if not isinstance(account, dict):
                continue

            url = _normalise_url(account)
            unreliable_endpoint = _is_unreliable_endpoint(url)
            has_metadata = _has_metadata(account)

            # Blackbird frequently emits API/search probe URLs that are not
            # durable user profiles; drop weak API-only hits early.
            if unreliable_endpoint and not has_metadata:
                continue

            confidence = _score_account(account)
            if confidence < float(min_confidence):
                continue

            platform = _normalise_platform(account)
            status = str(account.get("status") or "").strip().lower()

            claims.append(
                IdentityClaim(
                    platform=platform,
                    url=url,
                    username=default_username.lower(),
                    email=default_email,
                    confidence=round(confidence, 3),
                    tier=_to_tier(confidence),
                    verified=(status in {"found", "claimed", "active"} and not unreliable_endpoint),
                    source_tool="blackbird",
                    raw_data={
                        "blackbird_status": status,
                        "unreliable_endpoint": unreliable_endpoint,
                    },
                )
            )

        return claims

    def _build_command(
        self,
        query_type: str,
        query_value: str,
        export_pdf: bool,
        export_csv: bool,
        no_update: bool,
    ) -> list[str]:
        command = [self._binary, str(self._script_path), "--json"]

        if query_type == "username":
            command.extend(["--username", query_value])
        else:
            command.extend(["--email", query_value])

        if export_pdf:
            command.append("--pdf")
        if export_csv:
            command.append("--csv")
        if no_update:
            command.append("--no-update")

        return command

    def _resolve_artifact_paths(
        self,
        query_value: str,
        export_pdf: bool,
        export_csv: bool,
    ) -> dict[str, str]:
        date_raw = datetime.now().strftime("%m_%d_%Y")
        stem = f"{query_value}_{date_raw}_blackbird"
        result_dir = self._root / "results" / stem

        artifacts: dict[str, str] = {}
        if result_dir.exists():
            artifacts["directory"] = str(result_dir)

        json_path = result_dir / f"{stem}.json"
        if json_path.exists():
            artifacts["json"] = str(json_path)

        if export_pdf:
            pdf_path = result_dir / f"{stem}.pdf"
            if pdf_path.exists():
                artifacts["pdf"] = str(pdf_path)

        if export_csv:
            csv_path = result_dir / f"{stem}.csv"
            if csv_path.exists():
                artifacts["csv"] = str(csv_path)

        return artifacts

    def _ensure_runtime_paths(self) -> None:
        if not self._root.exists():
            raise RuntimeError(f"Blackbird root not found: {self._root}")
        if not self._script_path.exists():
            raise RuntimeError(f"Blackbird script not found: {self._script_path}")


def _resolve_query(username: str | None, email: str | None) -> tuple[str, str]:
    user = (username or "").strip()
    mail = (email or "").strip().lower()

    if user and mail:
        raise ValueError("Provide either username or email, not both")
    if user:
        return "username", user
    if mail:
        return "email", mail
    raise ValueError("Provide one of: username or email")


def _ensure_query_valid(query_type: str, query_value: str) -> None:
    if query_type == "email" and not _EMAIL_RE.match(query_value):
        raise ValueError("Email must be a valid address")


def _read_accounts_json(path: str | None) -> list[dict[str, Any]]:
    if not path:
        return []

    json_path = Path(path)
    if not json_path.exists():
        return []

    try:
        payload = json.loads(json_path.read_text(encoding="utf-8", errors="ignore"))
    except json.JSONDecodeError:
        logger.warning("BlackbirdSource: could not parse JSON artifact at %s", json_path)
        return []

    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def _tail_output(raw: bytes, max_chars: int = 1200) -> str:
    if not raw:
        return ""
    text = raw.decode("utf-8", errors="ignore").strip()
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _normalise_platform(account: dict[str, Any]) -> str:
    platform = str(account.get("name") or account.get("site") or "blackbird").strip().lower()
    platform = re.sub(r"\s+", "_", platform)
    return platform or "blackbird"


def _normalise_url(account: dict[str, Any]) -> str | None:
    value = str(account.get("url") or "").strip()
    return value or None


def _score_account(account: dict[str, Any]) -> float:
    score = 0.56
    url = _normalise_url(account)
    if url:
        score += 0.08

    status = str(account.get("status") or "").strip().lower()
    if status in {"found", "claimed", "active"}:
        score += 0.08

    if account.get("category"):
        score += 0.04

    if _has_metadata(account):
        score += 0.06

    # API/query probe endpoints frequently return false positives.
    if _is_unreliable_endpoint(url):
        score -= 0.30

    return min(score, 0.90)


def _has_metadata(account: dict[str, Any]) -> bool:
    metadata = account.get("metadata")
    if metadata is None:
        return False
    if isinstance(metadata, list):
        return len(metadata) > 0
    if isinstance(metadata, dict):
        return len(metadata) > 0
    return bool(metadata)


def _is_unreliable_endpoint(url: str | None) -> bool:
    if not url:
        return False

    try:
        parsed = urlparse(url)
    except ValueError:
        return False

    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()
    lower_url = url.lower()

    query_user_tokens = {
        "username",
        "usernames",
        "user",
        "account",
        "screen_name",
    }
    has_user_query = any(f"{token}=" in query for token in query_user_tokens)

    is_api_host = host.startswith("api") or ".api." in host

    if is_api_host and has_user_query:
        return True
    if "search" in path:
        return True
    if "validate" in path:
        return True
    if "/info/user=" in lower_url:
        return True

    return False


def _to_tier(confidence: float) -> str:
    if confidence >= 0.75:
        return "high"
    if confidence >= 0.45:
        return "medium"
    return "low"
