from __future__ import annotations

import argparse
import asyncio
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
	from playwright.async_api import TimeoutError as PlaywrightTimeoutError
	from playwright.async_api import async_playwright
except ModuleNotFoundError as exc:
	if exc.name == "playwright":
		raise SystemExit(
			"Missing dependency 'playwright'. Install with: pip install playwright && playwright install chromium"
		) from exc
	raise


URL_PATTERN = re.compile(r"https?://[^\s|>\]\)\"']+")


@dataclass(slots=True)
class GitHubProfileResult:
	url: str
	username: str
	account: dict[str, str | None]
	profile_readme: str | None
	profile_readme_source: str | None


@dataclass(slots=True)
class GitHubRepoResult:
	url: str
	owner: str
	repo: str
	readme: str | None


def _safe_text(value: str | None) -> str | None:
	if value is None:
		return None
	cleaned = value.strip()
	return cleaned or None


def extract_urls(text: str) -> list[str]:
	return URL_PATTERN.findall(text)


def normalize_github_url(url: str) -> str | None:
	try:
		parsed = urlparse(url.strip())
	except ValueError:
		return None

	if parsed.scheme not in {"http", "https"}:
		return None

	host = parsed.netloc.lower()
	if host not in {"github.com", "www.github.com"}:
		return None

	path = parsed.path.rstrip("/")
	if not path:
		return None

	return f"https://github.com{path}"


def split_github_targets(urls: list[str]) -> tuple[set[str], set[str]]:
	profiles: set[str] = set()
	repos: set[str] = set()

	for raw in urls:
		normalized = normalize_github_url(raw)
		if not normalized:
			continue

		parts = [p for p in urlparse(normalized).path.split("/") if p]
		if len(parts) == 1:
			profiles.add(normalized)
		elif len(parts) >= 2:
			repos.add(f"https://github.com/{parts[0]}/{parts[1]}")

	return profiles, repos


async def _first_text(page: Any, selectors: list[str]) -> str | None:
	for selector in selectors:
		try:
			handle = page.locator(selector).first
			if await handle.count() == 0:
				continue
			value = await handle.text_content(timeout=2000)
			cleaned = _safe_text(value)
			if cleaned:
				return cleaned
		except PlaywrightTimeoutError:
			continue
	return None


async def fetch_account_info(page: Any, profile_url: str) -> tuple[str, dict[str, str | None]]:
	response = await page.goto(profile_url, wait_until="domcontentloaded", timeout=25000)
	if not response:
		raise RuntimeError(f"No response while opening profile: {profile_url}")
	if response.status >= 400:
		raise RuntimeError(f"Profile returned HTTP {response.status}: {profile_url}")

	username = profile_url.rstrip("/").split("/")[-1]
	account = {
		"display_name": await _first_text(page, ["span.p-name", "h1.vcard-names span"]) ,
		"bio": await _first_text(page, ["div.p-note", "div.user-profile-bio div"]),
		"followers": await _first_text(page, ["a[href$='?tab=followers'] span.text-bold", "a[href$='?tab=followers'] span.Counter"]),
		"following": await _first_text(page, ["a[href$='?tab=following'] span.text-bold", "a[href$='?tab=following'] span.Counter"]),
		"repositories": await _first_text(page, ["a[href$='?tab=repositories'] span.Counter"]),
		"location": await _first_text(page, ["li[itemprop='homeLocation'] span.p-label"]),
		"company": await _first_text(page, ["li[itemprop='worksFor'] span.p-org"]),
	}

	return username, account


async def fetch_profile_readme(page: Any, username: str) -> tuple[str | None, str | None]:
	# GitHub profile README lives in repo <username>/<username> for most profiles.
	raw_urls = [
		f"https://raw.githubusercontent.com/{username}/{username}/HEAD/README.md",
		f"https://raw.githubusercontent.com/{username}/{username}/HEAD/Readme.md",
		f"https://raw.githubusercontent.com/{username}/{username}/HEAD/readme.md",
	]

	for raw_url in raw_urls:
		response = await page.goto(raw_url, wait_until="domcontentloaded", timeout=20000)
		if not response or response.status >= 400:
			continue
		text = _safe_text(await page.text_content("body"))
		if text:
			return text, raw_url

	return None, None


async def fetch_repo_readme(page: Any, repo_url: str) -> str | None:
	response = await page.goto(repo_url, wait_until="domcontentloaded", timeout=25000)
	if not response or response.status >= 400:
		return None

	candidates = [
		"article.markdown-body",
		"#readme article.markdown-body",
		"div.Box-body article.markdown-body",
	]

	text = await _first_text(page, candidates)
	if text:
		return text

	return None


async def scrape_github_data(urls: list[str], headless: bool = True) -> dict[str, Any]:
	profiles, repos = split_github_targets(urls)
	errors: list[str] = []

	profile_results: list[GitHubProfileResult] = []
	repo_results: list[GitHubRepoResult] = []

	async with async_playwright() as p:
		browser = await p.chromium.launch(headless=headless)
		context = await browser.new_context()
		page = await context.new_page()

		for profile_url in sorted(profiles):
			try:
				username, account = await fetch_account_info(page, profile_url)
				profile_readme, source = await fetch_profile_readme(page, username)
				profile_results.append(
					GitHubProfileResult(
						url=profile_url,
						username=username,
						account=account,
						profile_readme=profile_readme,
						profile_readme_source=source,
					)
				)
			except Exception as exc:  # noqa: BLE001 - keep run resilient on per-profile failures
				errors.append(f"Profile scrape failed for {profile_url}: {exc}")

		for repo_url in sorted(repos):
			try:
				parts = [p for p in urlparse(repo_url).path.split("/") if p]
				owner, repo = parts[0], parts[1]
				readme = await fetch_repo_readme(page, repo_url)
				repo_results.append(
					GitHubRepoResult(url=repo_url, owner=owner, repo=repo, readme=readme)
				)
			except Exception as exc:  # noqa: BLE001 - keep run resilient on per-repo failures
				errors.append(f"Repo scrape failed for {repo_url}: {exc}")

		await context.close()
		await browser.close()

	return {
		"generated_at": datetime.now(UTC).isoformat(),
		"input_url_count": len(urls),
		"github_profile_count": len(profile_results),
		"github_repo_count": len(repo_results),
		"profiles": [asdict(item) for item in profile_results],
		"repos": [asdict(item) for item in repo_results],
		"errors": errors,
	}


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Scrape GitHub profile and repository READMEs from links using Playwright."
	)
	parser.add_argument(
		"--url",
		action="append",
		default=[],
		help="Single URL to process. Can be repeated.",
	)
	parser.add_argument(
		"--text",
		default="",
		help="Raw text that may contain URLs (for extracted-link blobs).",
	)
	parser.add_argument(
		"--output",
		default="",
		help="Optional output JSON path.",
	)
	parser.add_argument(
		"--headless",
		action="store_true",
		default=True,
		help="Run browser in headless mode (default).",
	)
	parser.add_argument(
		"--show-browser",
		action="store_true",
		help="Show browser window while scraping.",
	)
	return parser.parse_args()


def _collect_input_urls(args: argparse.Namespace) -> list[str]:
	urls = list(args.url)
	if args.text:
		urls.extend(extract_urls(args.text))

	deduped: list[str] = []
	seen: set[str] = set()
	for url in urls:
		if url not in seen:
			seen.add(url)
			deduped.append(url)
	return deduped


def main() -> None:
	args = parse_args()
	urls = _collect_input_urls(args)
	if not urls:
		raise SystemExit("No URLs provided. Use --url or --text.")

	result = asyncio.run(scrape_github_data(urls=urls, headless=not args.show_browser))

	payload = json.dumps(result, indent=2, ensure_ascii=False)
	if args.output:
		output_path = Path(args.output)
		output_path.parent.mkdir(parents=True, exist_ok=True)
		output_path.write_text(payload, encoding="utf-8")
		print(f"Wrote results to {output_path}")
	else:
		print(payload)


if __name__ == "__main__":
	main()
