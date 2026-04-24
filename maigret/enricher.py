from __future__ import annotations

import json
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from maigret.intake import ResumeParseError, parse_resume_bytes
from maigret.runner import normalize_username

try:
    from flowsint_core.core.enricher_base import Enricher
    from flowsint_core.core.logger import Logger
    from flowsint_enrichers.registry import flowsint_enricher
    from flowsint_types import Username
    from flowsint_types.social_account import SocialAccount
except ImportError:  # pragma: no cover - fallback for this repository
    class Enricher:  # type: ignore[no-redef]
        """Lightweight fallback when Flowsint base classes are unavailable."""

        _graph_service = None
        sketch_id = "maigret-enricher"

    class Logger:  # type: ignore[no-redef]
        """Fallback logger interface matching Flowsint usage."""

        @staticmethod
        def error(sketch_id: str, payload: dict[str, Any]) -> None:
            print(f"[{sketch_id}] ERROR: {payload.get('message')}")

    def flowsint_enricher(cls: type[Any]) -> type[Any]:
        return cls

    @dataclass(frozen=True)
    class Username:  # type: ignore[no-redef]
        value: str

    @dataclass
    class SocialAccount:  # type: ignore[no-redef]
        username: Username
        profile_url: str
        platform: str
        profile_picture_url: str | None = None
        bio: str | None = None
        followers_count: int | None = None
        following_count: int | None = None
        posts_count: int | None = None


FALSE_POSITIVES = {"leagueoflegends"}


@dataclass(frozen=True)
class ResumeInput:
    """Resume payload used by the resume-first enricher flow."""

    content: bytes
    filename: str
    content_type: str | None = None
    username_hint: str | None = None


@flowsint_enricher
class MaigretEnricher(Enricher):
    """Resume-first Maigret enricher with optional Username fallback input."""

    InputType = ResumeInput
    OutputType = SocialAccount

    @classmethod
    def name(cls) -> str:
        return "resume_to_socials_maigret"

    @classmethod
    def category(cls) -> str:
        return "social"

    @classmethod
    def key(cls) -> str:
        return "resume"

    def run_maigret(self, username: str) -> Path:
        safe_username = username.replace("/", "_").replace("\\", "_")
        output_dir = Path(tempfile.gettempdir())
        output_file = output_dir / f"report_{safe_username}_simple.json"
        output_file.unlink(missing_ok=True)

        try:
            subprocess.run(
                ["maigret", username, "-J", "simple", "-fo", str(output_dir)],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
        except Exception as exc:
            Logger.error(
                self.sketch_id,
                {"message": f"Maigret execution failed for {username}: {exc}"},
            )
        return output_file

    def parse_maigret_output(self, username_obj: Username, output_file: Path) -> list[SocialAccount]:
        results: list[SocialAccount] = []
        if not output_file.exists():
            return results

        try:
            with output_file.open("r", encoding="utf-8") as file_obj:
                raw_data = json.load(file_obj)
        except Exception as exc:
            Logger.error(
                self.sketch_id,
                {"message": f"Failed to load output file for {username_obj.value}: {exc}"},
            )
            return results

        for platform, profile in raw_data.items():
            if profile.get("status", {}).get("status") != "Claimed":
                continue

            if platform.lower() in FALSE_POSITIVES:
                continue

            status = profile.get("status", {})
            ids = status.get("ids", {})
            profile_url = status.get("url") or profile.get("url_user")
            if not profile_url:
                continue

            try:
                followers = int(ids.get("follower_count")) if ids.get("follower_count") else None
                following = int(ids.get("following_count")) if ids.get("following_count") else None
                posts = (
                    int(ids.get("public_repos_count", 0)) + int(ids.get("public_gists_count", 0))
                    if "public_repos_count" in ids or "public_gists_count" in ids
                    else None
                )
            except ValueError:
                followers = None
                following = None
                posts = None

            try:
                results.append(
                    SocialAccount(
                        username=username_obj,
                        profile_url=profile_url,
                        platform=platform,
                        profile_picture_url=ids.get("image"),
                        bio=None,
                        followers_count=followers,
                        following_count=following,
                        posts_count=posts,
                    )
                )
            except Exception as exc:
                Logger.error(
                    self.sketch_id,
                    {
                        "message": (
                            f"Failed to create SocialAccount for {username_obj.value} "
                            f"on {platform}: {exc}"
                        )
                    },
                )
                continue

        return results

    def _to_username_obj(self, value: str) -> Username:
        try:
            return Username(value=value)
        except TypeError:
            return Username(value)  # type: ignore[misc]

    def _extract_usernames_from_resume(self, resume_input: ResumeInput) -> list[Username]:
        candidates: list[str] = []

        if resume_input.username_hint:
            normalized_hint = normalize_username(resume_input.username_hint)
            if normalized_hint:
                candidates.append(normalized_hint)

        try:
            parsed = parse_resume_bytes(
                content=resume_input.content,
                filename=resume_input.filename,
                content_type=resume_input.content_type,
            )
        except ResumeParseError as exc:
            Logger.error(
                self.sketch_id,
                {"message": f"Resume parsing failed for {resume_input.filename}: {exc}"},
            )
            return [self._to_username_obj(v) for v in dict.fromkeys(candidates)]

        for username in parsed.usernames:
            normalized = normalize_username(username)
            if normalized:
                candidates.append(normalized)

        for link in parsed.links:
            if link.platform.lower() != "github":
                continue
            path = urlparse(link.url).path.strip("/")
            handle = path.split("/", 1)[0] if path else ""
            normalized = normalize_username(handle)
            if normalized:
                candidates.append(normalized)

        deduped = [self._to_username_obj(value) for value in dict.fromkeys(candidates)]
        return deduped

    async def scan(self, data: list[InputType | Username | str]) -> list[OutputType]:
        results: list[OutputType] = []
        for item in data:
            usernames: list[Username] = []

            if isinstance(item, ResumeInput):
                usernames = self._extract_usernames_from_resume(item)
            elif isinstance(item, str):
                normalized = normalize_username(item)
                if normalized:
                    usernames = [self._to_username_obj(normalized)]
            elif hasattr(item, "value"):
                value = normalize_username(str(getattr(item, "value", "")))
                if value:
                    usernames = [self._to_username_obj(value)]

            for username_obj in usernames:
                if not username_obj.value:
                    continue
                try:
                    output_file = self.run_maigret(username_obj.value)
                    parsed = self.parse_maigret_output(username_obj, output_file)
                    results.extend(parsed)
                except Exception as exc:
                    Logger.error(
                        self.sketch_id,
                        {"message": f"Failed to process username {username_obj.value}: {exc}"},
                    )
                    continue

        return results

    def postprocess(self, results: list[OutputType], original_input: list[InputType]) -> list[OutputType]:
        graph_service = getattr(self, "_graph_service", None)
        if not graph_service:
            return results

        create_node = getattr(self, "create_node", None)
        create_relationship = getattr(self, "create_relationship", None)
        log_graph_message = getattr(self, "log_graph_message", None)
        if not callable(create_node) or not callable(create_relationship) or not callable(log_graph_message):
            return results

        for profile in results:
            try:
                create_node(profile.username)
                create_node(profile)
                create_relationship(profile.username, profile, "HAS_SOCIAL_ACCOUNT")
                log_graph_message(f"{profile.username.value} -> account found on {profile.platform}")
            except Exception as exc:
                Logger.error(
                    self.sketch_id,
                    {
                        "message": (
                            f"Failed to create graph nodes for {profile.username.value} "
                            f"on {profile.platform}: {exc}"
                        )
                    },
                )
                continue

        return results


InputType = MaigretEnricher.InputType
OutputType = MaigretEnricher.OutputType