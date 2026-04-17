"""
events/raw_publisher.py — Shared fire-and-forget raw-claim Kafka publisher.

Every orchestrator source (maigret, github, github_octosuite, holehe) calls
``publish_raw_claims()`` immediately after its search so that all raw
IdentityClaim signals land in their respective topics *before* the
orchestrator consolidates them into Splink.

Topic routing:
  source_tool="maigret"            → osint.raw.maigret.v1
  source_tool="github"             → osint.raw.github.v1
  source_tool="github_octosuite"   → osint.raw.github.v1
  source_tool="holehe"             → osint.raw.holehe.v1
  source_tool=<other>              → osint.raw.<source_tool>.v1
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from maigret.agent.report import IdentityClaim

logger = logging.getLogger(__name__)

_TOPIC_MAP: dict[str, str] = {
    "maigret":          "osint.raw.maigret.v1",
    "github":           "osint.raw.github.v1",
    "github_octosuite": "osint.raw.github.v1",   # both sources share the same audit topic
    "holehe":           "osint.raw.holehe.v1",
}


def _topic_for(source_tool: str) -> str:
    return _TOPIC_MAP.get(source_tool.lower(), f"osint.raw.{source_tool.lower()}.v1")


def build_raw_claims_event(
    claims: list[IdentityClaim],
    *,
    subject: str,
    source_tool: str,
    trigger: str = "orchestrator",
) -> tuple[bytes, str]:
    """
    Serialise a list of IdentityClaim objects into a raw-source Kafka event.

    Envelope schema mirrors osint.raw.academic.v1 so downstream consumers
    see a uniform structure across all raw topics.

    Returns (payload_bytes, kafka_key).
    Key = subject (username / name / email) for per-subject ordering.
    """
    now = datetime.now(tz=timezone.utc).isoformat()

    claim_dicts: list[dict[str, Any]] = []
    skipped = 0
    for c in claims:
        try:
            claim_dicts.append({
                "platform":         c.platform,
                "url":              c.url,
                "username":         c.username,
                "email":            c.email,
                "email_hash":       c.email_hash,
                "phone":            c.phone,
                "confidence":       c.confidence,
                "tier":             c.tier,
                "verified":         c.verified,
                "source_tool":      c.source_tool,
                "institutions":     c.institutions,
                "research_domains": c.research_domains,
                "co_authors":       c.co_authors,
            })
        except Exception:
            skipped += 1

    event: dict[str, Any] = {
        "schema_version": "1.0",
        "event_type":     f"osint.raw.{source_tool}",
        "event_id":       str(uuid.uuid4()),
        "emitted_at":     now,

        "query": {
            "subject":     subject,
            "source":      source_tool,
            "trigger":     trigger,
        },

        "stats": {
            "total_claims":      len(claim_dicts) + skipped,
            "total_emitted":     len(claim_dicts),
            "skipped_malformed": skipped,
        },

        "claims": claim_dicts,
    }

    payload_bytes = json.dumps(event, ensure_ascii=False).encode("utf-8")
    return payload_bytes, subject


def publish_raw_claims(
    claims: list[IdentityClaim],
    *,
    subject: str,
    source_tool: str,
    trigger: str = "orchestrator",
) -> bool:
    """
    Build and publish a raw-claims event to the source's Kafka topic.

    Soft-fail: always returns True/False, never raises.
    Callers must treat False as a non-fatal log event, not an error.
    """
    if not claims:
        return True  # nothing to publish; not an error

    try:
        from maigret.kafka.producer import get_kafka_producer

        kafka = get_kafka_producer()
        topic = _topic_for(source_tool)

        payload_bytes, key = build_raw_claims_event(
            claims,
            subject=subject,
            source_tool=source_tool,
            trigger=trigger,
        )

        kafka.publish(payload_bytes, key=key, topic=topic)
        logger.info(
            "Raw claims published | source=%s topic=%s claims=%d bytes=%d",
            source_tool, topic, len(claims), len(payload_bytes),
        )
        return True

    except Exception as exc:
        logger.error(
            "Raw claims publish failed | source=%s subject=%s | %s",
            source_tool, subject, exc,
        )
        return False
