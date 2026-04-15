"""
orchestrator/h8mail_source.py - H8mail email breach datasource adapter.

Runs the h8mail CLI for an email target and emits an IdentityClaim when
breach signals are present.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

from maigret.agent.report import IdentityClaim

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class H8mailSource:
    """Run h8mail for email-breach signals and convert to IdentityClaim."""

    def __init__(
        self,
        binary: str | None = None,
        request_timeout: int = 45,
    ) -> None:
        configured_binary = (binary or os.getenv("H8MAIL_BINARY") or "").strip()
        if configured_binary:
            self._binary = configured_binary
        else:
            venv_script = Path(sys.prefix).resolve() / "bin" / "h8mail"
            self._binary = str(venv_script) if venv_script.exists() else "h8mail"
        self._request_timeout = max(10, int(request_timeout))

    async def search(self, email: str, min_confidence: float = 0.45) -> list[IdentityClaim]:
        """Run h8mail and return one aggregate claim when breaches are detected."""
        target = email.strip().lower()
        if not _looks_like_email(target):
            return []

        payload = await self._run_h8mail(target)
        if not _has_findings(payload):
            logger.info("H8mailSource: email=%s returned no findings", target)
            return []

        breach_count = _extract_breach_count(payload)
        confidence = _score_result(payload=payload, breach_count=breach_count)
        if confidence < float(min_confidence):
            return []

        username = _extract_username(payload) or target.split("@", 1)[0]
        platforms = _extract_platforms(payload)
        claims: list[IdentityClaim] = []
        for platform in platforms:
            claims.append(
                IdentityClaim(
                    platform=platform,
                    username=username.lower(),
                    email=target,
                    confidence=round(confidence, 3),
                    tier=_to_tier(confidence),
                    verified=True,
                    source_tool="h8mail",
                )
            )

        if not claims:
            claims = [
                IdentityClaim(
                    platform="h8mail",
                    username=username.lower(),
                    email=target,
                    confidence=round(confidence, 3),
                    tier=_to_tier(confidence),
                    verified=True,
                    source_tool="h8mail",
                )
            ]

        logger.info(
            "H8mailSource: email=%s confidence=%.2f breaches=%d claims=%d",
            target,
            confidence,
            breach_count,
            len(claims),
        )
        return claims

    async def _run_h8mail(self, email: str) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="h8mail_") as tmpdir:
            output_path = Path(tmpdir) / "h8mail_output.json"

            proc = await self._spawn_h8mail_process(email=email, output_path=output_path)

            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=self._request_timeout,
                )
            except asyncio.TimeoutError as exc:
                proc.kill()
                raise RuntimeError(
                    f"h8mail timed out after {self._request_timeout}s"
                ) from exc

            if proc.returncode != 0:
                stderr_text = stderr.decode("utf-8", errors="ignore").strip()
                stdout_text = stdout.decode("utf-8", errors="ignore").strip()
                details = stderr_text or stdout_text or "no output"
                raise RuntimeError(f"h8mail failed with exit code {proc.returncode}: {details}")

            if output_path.exists():
                output = output_path.read_text(encoding="utf-8", errors="ignore").strip()
                if output:
                    parsed = _parse_json_output(output)
                    if isinstance(parsed, dict):
                        return parsed
                    if isinstance(parsed, list):
                        return {"results": parsed}
                    return {"value": parsed}

            stdout_text = stdout.decode("utf-8", errors="ignore").strip()
            if not stdout_text:
                return {}

            parsed = _parse_json_output(stdout_text)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {"results": parsed}
            return {"value": parsed}

    async def _spawn_h8mail_process(
        self,
        email: str,
        output_path: Path,
    ) -> asyncio.subprocess.Process:
        args = ("-t", email, "-j", str(output_path))
        venv_script = str(Path(sys.prefix).resolve() / "bin" / "h8mail")
        sibling_script = str((Path(sys.executable).resolve().parent / "h8mail"))

        try:
            return await asyncio.create_subprocess_exec(
                self._binary,
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            logger.warning(
                "h8mail binary '%s' not found; retrying via '%s'",
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
                except FileNotFoundError:
                    try:
                        return await asyncio.create_subprocess_exec(
                            sys.executable,
                            "-m",
                            "h8mail",
                            *args,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                    except FileNotFoundError as fallback_exc:
                        raise RuntimeError(
                            "h8mail executable not found. Tried "
                            f"'{self._binary}', '{venv_script}', '{sibling_script}', and '{sys.executable} -m h8mail'. "
                            "Install h8mail in the active environment or set H8MAIL_BINARY."
                        ) from fallback_exc


def _looks_like_email(value: str) -> bool:
    return bool(_EMAIL_RE.match(value or ""))


def _parse_json_output(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    start_idx = -1
    end_idx = -1
    for i, ch in enumerate(raw):
        if ch in "[{":
            start_idx = i
            break

    for i in range(len(raw) - 1, -1, -1):
        if raw[i] in "]}":
            end_idx = i
            break

    if start_idx != -1 and end_idx > start_idx:
        snippet = raw[start_idx:end_idx + 1]
        try:
            return json.loads(snippet)
        except json.JSONDecodeError:
            return {}

    return {}


def _has_findings(payload: dict[str, Any]) -> bool:
    findings = _extract_breach_entries(payload)
    if findings:
        return True

    for key in ("count", "total", "total_results", "breach_count"):
        value = payload.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return True

    found = payload.get("found")
    if isinstance(found, bool):
        return found
    if isinstance(found, (int, float)):
        return found > 0

    nested_payload = dict(payload)
    nested_payload.pop("targets", None)
    return _has_non_empty_nested_collection(nested_payload)


def _has_non_empty_nested_collection(value: Any, depth: int = 0) -> bool:
    if depth > 4:
        return False

    if isinstance(value, list):
        return len(value) > 0

    if isinstance(value, dict):
        ignore = {
            "query", "email", "message", "status", "success", "errors", "version",
        }
        for key, child in value.items():
            if key in ignore:
                continue
            if _has_non_empty_nested_collection(child, depth + 1):
                return True
    return False


def _extract_breach_count(payload: dict[str, Any]) -> int:
    findings = _extract_breach_entries(payload)
    if findings:
        return len(findings)

    for key in ("breach_count", "count", "total", "total_results"):
        value = payload.get(key)
        if isinstance(value, int):
            return max(0, value)

    targets = payload.get("targets")
    if isinstance(targets, list):
        total = 0
        for entry in targets:
            if not isinstance(entry, dict):
                continue
            pwn_num = entry.get("pwn_num")
            if isinstance(pwn_num, (int, float)) and pwn_num > 0:
                total += int(pwn_num)
        if total > 0:
            return total

    return 1


def _extract_breach_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []

    for key in ("breaches", "pwned", "results", "records", "data"):
        value = payload.get(key)
        entries.extend(_to_entry_dicts(value))

    targets = payload.get("targets")
    if isinstance(targets, list):
        for target in targets:
            if not isinstance(target, dict):
                continue
            entries.extend(_to_entry_dicts(target.get("data")))

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries:
        key = json.dumps(entry, sort_keys=True, ensure_ascii=True)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


def _to_entry_dicts(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        out: list[dict[str, Any]] = []
        for item in value:
            if isinstance(item, dict):
                out.append(item)
            elif isinstance(item, str) and item.strip():
                out.append({"name": item.strip()})
        return out

    if isinstance(value, dict):
        items = value.get("items")
        if isinstance(items, list):
            return _to_entry_dicts(items)
        return [value]

    return []


def _extract_platforms(payload: dict[str, Any]) -> list[str]:
    platforms: list[str] = []
    seen: set[str] = set()
    for entry in _extract_breach_entries(payload):
        platform = _normalise_platform_name(_extract_platform_name(entry))
        if not platform or platform in seen:
            continue
        seen.add(platform)
        platforms.append(platform)
    return platforms


def _extract_platform_name(entry: dict[str, Any]) -> str:
    for key in (
        "name",
        "breach",
        "site",
        "domain",
        "service",
        "source",
        "title",
        "platform",
        "database",
        "host",
    ):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _normalise_platform_name(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""

    # Domain-like names are common in breach payloads.
    raw = raw.replace("https://", "").replace("http://", "").strip("/")
    if "." in raw:
        raw = raw.split("/")[0]

    raw = re.sub(r"[^a-z0-9._-]+", "_", raw)
    raw = raw.strip("._-")
    return raw


def _extract_username(payload: dict[str, Any]) -> str | None:
    key_candidates = {
        "username", "user", "handle", "login", "account", "screen_name",
    }

    queue: list[Any] = [payload]
    seen_ids: set[int] = set()

    while queue:
        current = queue.pop(0)
        obj_id = id(current)
        if obj_id in seen_ids:
            continue
        seen_ids.add(obj_id)

        if isinstance(current, dict):
            for key, value in current.items():
                if key.lower() in key_candidates and isinstance(value, str):
                    cleaned = value.strip()
                    if cleaned:
                        return cleaned
                if isinstance(value, (dict, list)):
                    queue.append(value)
        elif isinstance(current, list):
            queue.extend(item for item in current if isinstance(item, (dict, list)))

    return None


def _score_result(payload: dict[str, Any], breach_count: int) -> float:
    score = 0.54

    if breach_count > 0:
        score += 0.14
    if breach_count >= 5:
        score += 0.10
    if breach_count >= 10:
        score += 0.08

    if isinstance(payload.get("sources"), list) and len(payload["sources"]) >= 2:
        score += 0.06

    return min(score, 0.90)


def _to_tier(confidence: float) -> str:
    if confidence >= 0.75:
        return "high"
    if confidence >= 0.45:
        return "medium"
    return "low"
