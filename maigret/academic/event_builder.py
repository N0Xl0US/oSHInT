"""
academic/event_builder.py — Build osint.raw.academic.v1 Kafka events.

Follows the same envelope schema as other raw-source event builders so all
topics are structurally consistent for downstream consumers.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from maigret.academic.config import get_academic_config
from maigret.academic.models import AcademicAuthorProfile
from maigret.kafka.producer import get_kafka_producer

logger = logging.getLogger(__name__)


def build_academic_event(
    query_name: str,
    profiles: list[AcademicAuthorProfile],
    query_institution: str | None = None,
    sources_queried: int = 2,
    trigger: str = "agent",
) -> tuple[bytes, str]:
    """
    Build a Kafka-ready osint.raw.academic.v1 event.

    Args:
        query_name:        The author name that was searched.
        profiles:          Scored AcademicAuthorProfile objects from PPF.
        query_institution: Optional institution filter used in the query.
        sources_queried:   Number of data sources queried.
        trigger:           What initiated the search ('agent', 'api', 'test').

    Returns:
        (payload_bytes, kafka_key)
    """
    now = datetime.now(tz=timezone.utc).isoformat()

    profile_dicts: list[dict[str, Any]] = []
    skipped = 0
    for p in profiles:
        try:
            profile_dicts.append({
                "source":         p.source,
                "author_id":      p.author_id,
                "name":           p.name,
                "affiliations":   p.affiliations,
                "orcid":          p.orcid,
                "homepage":       p.homepage,
                "paper_count":    p.paper_count,
                "citation_count": p.citation_count,
                "h_index":        p.h_index,
                "confidence":     p.confidence,
                "research_domains": p.research_domains,
                "top_papers": [
                    {
                        "title":          paper.title,
                        "year":           paper.year,
                        "doi":            paper.doi,
                        "venue":          paper.venue,
                        "citation_count": paper.citation_count,
                    }
                    for paper in p.top_papers
                ],
                "co_authors": [
                    {"name": ca.name, "author_id": ca.author_id}
                    for ca in p.co_authors
                ],
            })
        except Exception:
            skipped += 1

    event: dict[str, Any] = {
        "schema_version":  "1.0",
        "event_type":      "osint.raw.academic",
        "event_id":        str(uuid.uuid4()),
        "emitted_at":      now,

        "query": {
            "name":        query_name,
            "institution": query_institution,
            "source":      "academic",
            "trigger":     trigger,
        },

        "stats": {
            "total_authors_found": len(profile_dicts) + skipped,
            "total_filtered":      len(profile_dicts),
            "skipped_malformed":   skipped,
            "sources_queried":     sources_queried,
        },

        "profiles": profile_dicts,
    }

    payload_bytes = json.dumps(event, ensure_ascii=False).encode("utf-8")
    return payload_bytes, query_name


def publish_academic_event(
    query_name: str,
    profiles: list[AcademicAuthorProfile],
    query_institution: str | None = None,
    sources_queried: int = 2,
    trigger: str = "agent",
) -> bool:
    """
    Build and publish an academic event to osint.raw.academic.v1.

    Returns True if published successfully, False on any failure.
    Soft error: callers must never raise on False.
    """
    try:
        config = get_academic_config()
        kafka = get_kafka_producer()

        payload_bytes, key = build_academic_event(
            query_name=query_name,
            profiles=profiles,
            query_institution=query_institution,
            sources_queried=sources_queried,
            trigger=trigger,
        )

        kafka.publish(payload_bytes, key=key, topic=config.academic_kafka_topic)
        logger.info(
            "Kafka academic event published | topic=%s key=%s bytes=%d",
            config.academic_kafka_topic, key, len(payload_bytes),
        )
        return True

    except Exception as exc:
        logger.error("Kafka academic publish failed: %s", exc)
        return False
