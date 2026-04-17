"""
tools.py — Groq-compatible tool definitions + executor dispatch.

Each tool has:
  1. A JSON schema (OpenAI function-calling format) for the LLM
  2. An async executor function that calls the OSINT server via HTTP
"""

from __future__ import annotations

import json
import logging
from typing import Any

from maigret.agent.client import OsintClient

logger = logging.getLogger(__name__)


# ── Tool schemas (OpenAI function-calling format) ────────────────────────────

TOOL_SEARCH_USERNAME = {
    "type": "function",
    "function": {
        "name": "search_username",
        "description": (
            "Run a full OSINT pipeline search for a username across 150+ platforms. "
            "Returns scored identity claims with confidence levels, platform tiers, "
            "and HTTP verification status. Use this to investigate a person's online "
            "presence by their known username."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "username": {
                    "type": "string",
                    "description": "The username to search for (e.g. 'elonmusk', 'johndoe_42')",
                },
                "verify_http": {
                    "type": "boolean",
                    "description": "Whether to HTTP HEAD verify each discovered profile URL. Slower but more accurate.",
                    "default": False,
                },
                "min_confidence": {
                    "type": "number",
                    "description": "Minimum confidence score (0.0–1.0) to include in results.",
                    "default": 0.3,
                },
            },
            "required": ["username"],
        },
    },
}

TOOL_PARSE_URL = {
    "type": "function",
    "function": {
        "name": "parse_profile_url",
        "description": (
            "Extract the platform name and username from a profile URL. "
            "Useful when you discover a URL and need to identify which "
            "platform it belongs to."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The profile URL to parse (e.g. 'https://github.com/torvalds')",
                },
            },
            "required": ["url"],
        },
    },
}

TOOL_CHECK_HEALTH = {
    "type": "function",
    "function": {
        "name": "check_server_health",
        "description": "Check if the OSINT extraction server is online and ready to accept searches.",
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
}

TOOL_SEARCH_ACADEMIC = {
    "type": "function",
    "function": {
        "name": "search_academic",
        "description": (
            "Search for academic publications authored by a person across "
            "Semantic Scholar and OpenAlex, plus Orchid when configured. "
            "Returns author profiles with "
            "affiliations, ORCID identifiers, top papers, co-author networks, "
            "and confidence scores. Use this to investigate a subject's "
            "academic research footprint."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Full name of the author to search for (e.g. 'Jane Smith')",
                },
                "institution": {
                    "type": "string",
                    "description": "Optional institution or university to narrow results (e.g. 'MIT')",
                },
            },
            "required": ["name"],
        },
    },
}

TOOL_SEARCH_GITHUB = {
    "type": "function",
    "function": {
        "name": "search_github",
        "description": (
            "Search GitHub for a specific username and return a verified account claim "
            "using public profile metadata (repos, followers, account age). Use this "
            "for fast, focused validation when GitHub presence matters."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "username": {
                    "type": "string",
                    "description": "The GitHub username to check",
                },
                "min_confidence": {
                    "type": "number",
                    "description": "Minimum confidence score (0.0–1.0) required in output",
                    "default": 0.35,
                },
            },
            "required": ["username"],
        },
    },
}

TOOL_RESOLVE_IDENTITY = {
    "type": "function",
    "function": {
        "name": "resolve_identity",
        "description": (
            "Run a full identity resolution pipeline: fans out to Maigret, "
            "Academic, GitHub, Holehe, and LinkedIn URL intelligence in parallel, collects all identity claims, "
            "feeds them into Splink for entity resolution, builds golden records, "
            "and publishes to identity.resolved. Use this for comprehensive "
            "identity resolution that combines all available OSINT sources."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "username": {
                    "type": "string",
                    "description": "Optional username to investigate across all sources",
                },
                "name": {
                    "type": "string",
                    "description": "Optional full name of the person (used for academic search and username derivation)",
                },
                "email": {
                    "type": "string",
                    "description": "Optional email address (used by email-capable sources and as a username hint fallback)",
                },
                "linkedin_url": {
                    "type": "string",
                    "description": "Optional LinkedIn profile URL (e.g. https://linkedin.com/in/username)",
                },
                "institution": {
                    "type": "string",
                    "description": "Optional institution to narrow academic results (e.g. 'MIT')",
                },
            },
            "required": [],
        },
    },
}

# All tools as a list for Groq
ALL_TOOLS = [
    TOOL_SEARCH_USERNAME,
    TOOL_SEARCH_ACADEMIC,
    TOOL_SEARCH_GITHUB,
    TOOL_RESOLVE_IDENTITY,
    TOOL_PARSE_URL,
    TOOL_CHECK_HEALTH,
]


def get_tools_for_context(
    *,
    username: str | None = None,
    name: str | None = None,
    email: str | None = None,
) -> list[dict[str, Any]]:
    """Return tools ordered by input-signal strength for this investigation context."""
    ordered: list[dict[str, Any]] = []
    added: set[str] = set()

    def add(tool: dict[str, Any]) -> None:
        tool_name = tool.get("function", {}).get("name")
        if tool_name and tool_name not in added:
            ordered.append(tool)
            added.add(tool_name)

    if name:
        add(TOOL_SEARCH_ACADEMIC)
    if (name or email) and (username or name or email):
        add(TOOL_RESOLVE_IDENTITY)
    if username:
        add(TOOL_SEARCH_USERNAME)
        add(TOOL_SEARCH_GITHUB)

    add(TOOL_PARSE_URL)
    add(TOOL_CHECK_HEALTH)

    # Keep any future tools available even if not explicitly prioritized above.
    for tool in ALL_TOOLS:
        add(tool)

    return ordered


# ── Tool executors ───────────────────────────────────────────────────────────


async def execute_tool(
    tool_name: str,
    arguments: dict[str, Any],
    client: OsintClient,
) -> str:
    """
    Execute a tool call and return the result as a JSON string.

    This is called when Groq emits a tool_calls block.
    """
    logger.info("Executing tool: %s(%s)", tool_name, json.dumps(arguments)[:200])

    try:
        if tool_name == "search_username":
            result = await client.search_username_filtered(
                username=arguments["username"],
                verify_http=arguments.get("verify_http", False),
                min_confidence=arguments.get("min_confidence", 0.3),
            )
            # Summarise for the LLM to keep context window manageable
            return json.dumps({
                "query": result.get("query"),
                "pipeline": result.get("pipeline"),
                "profiles": result.get("profiles", []),
            }, indent=2)

        elif tool_name == "parse_profile_url":
            result = await client.parse_url(url=arguments["url"])
            return json.dumps(result, indent=2)

        elif tool_name == "search_academic":
            from maigret.academic.runner import AcademicSearchTool
            ac_tool = AcademicSearchTool()
            result = await ac_tool.search_academic(
                name=arguments["name"],
                institution=arguments.get("institution"),
            )
            return json.dumps({
                "tool": "academic",
                "query_name": result.query_name,
                "query_institution": result.query_institution,
                "total_authors_found": result.total_authors_found,
                "profiles": [
                    {
                        "source":         p.source,
                        "name":           p.name,
                        "affiliations":   p.affiliations,
                        "orcid":          p.orcid,
                        "paper_count":    p.paper_count,
                        "citation_count": p.citation_count,
                        "h_index":        p.h_index,
                        "confidence":     p.confidence,
                        "top_papers": [
                            {"title": tp.title, "year": tp.year, "citations": tp.citation_count}
                            for tp in p.top_papers[:3]
                        ],
                        "co_authors": [ca.name for ca in p.co_authors[:5]],
                    }
                    for p in result.profiles
                ],
            }, indent=2)

        elif tool_name == "search_github":
            from maigret.orchestrator.github_source import GitHubSource

            source = GitHubSource()
            claims = await source.search(
                username=arguments["username"],
                min_confidence=float(arguments.get("min_confidence", 0.35)),
            )
            return json.dumps({
                "tool": "github",
                "username": arguments["username"],
                "total_claims": len(claims),
                "profiles": [
                    {
                        "platform": c.platform,
                        "url": c.url,
                        "username": c.username,
                        "confidence": c.confidence,
                        "tier": c.tier,
                        "verified": c.verified,
                        "email": c.email,
                        "institutions": c.institutions,
                        "source": c.source_tool,
                    }
                    for c in claims
                ],
            }, indent=2)

        elif tool_name == "resolve_identity":
            from maigret.orchestrator import IdentityOrchestrator
            orchestrator = IdentityOrchestrator()
            orch_result = await orchestrator.resolve_identity(
                username=arguments.get("username"),
                name=arguments.get("name"),
                email=arguments.get("email"),
                linkedin_url=arguments.get("linkedin_url"),
                institution=arguments.get("institution"),
            )
            return json.dumps(orch_result.to_summary_dict(), indent=2)

        elif tool_name == "check_server_health":
            healthy = await client.health_check()
            return json.dumps({"status": "online" if healthy else "offline"})

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    except Exception as exc:
        logger.error("Tool execution failed: %s — %s", tool_name, exc)
        return json.dumps({"error": str(exc)})
