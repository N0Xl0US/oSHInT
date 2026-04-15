"""
resolver/publisher.py — Publish GoldenRecords to Kafka identity.resolved topic.

Uses the envelope format from handover.md §2.
Routes records to identity.resolved or identity.quarantine based on
confidence thresholds from handover.md §1.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from maigret.agent.report import GoldenRecord
from maigret.resolver.config import ResolverConfig, get_resolver_config

logger = logging.getLogger(__name__)


def build_resolved_event(
    golden_record: GoldenRecord,
    trigger: str = "agent",
) -> tuple[bytes, str]:
    """
    Build a Kafka-ready identity.resolved event.

    Uses the envelope format from handover.md §2:
    {
      "envelope": { message_id, topic, schema_version, emitted_at, source_phase },
      "payload": { <golden_record> }
    }

    Returns (payload_bytes, kafka_key).
    Key = entity_id for per-entity ordering guarantees (handover.md §2 partitioning).
    """
    now = datetime.now(tz=timezone.utc).isoformat()

    # Determine target topic based on quality flags
    config = get_resolver_config()
    if golden_record.quality_flags.quarantined:
        topic = config.quarantine_topic
    else:
        topic = config.resolved_topic

    event: dict[str, Any] = {
        "envelope": {
            "message_id":     str(uuid.uuid4()),
            "topic":          topic,
            "schema_version": "1.0",
            "emitted_at":     now,
            "source_phase":   "phase_1",
            "trigger":        trigger,
        },
        "payload": golden_record.model_dump(mode="json"),
    }

    payload_bytes = json.dumps(event, ensure_ascii=False).encode("utf-8")
    return payload_bytes, golden_record.entity_id


def publish_golden_records(
    golden_records: list[GoldenRecord],
    trigger: str = "agent",
) -> dict[str, int]:
    """
    Publish golden records to their appropriate Kafka topics.

    Routes to:
    - identity.resolved    (confidence >= quarantine threshold)
    - identity.quarantine  (confidence < quarantine threshold)

    Returns {"resolved": N, "quarantined": M, "failed": F}.
    """
    stats = {"resolved": 0, "quarantined": 0, "failed": 0}

    try:
        from maigret.kafka.producer import get_producer
        producer = get_producer()
        if not producer.is_healthy():
            logger.warning("Kafka producer not healthy — skipping publish")
            stats["failed"] = len(golden_records)
            return stats
    except Exception as exc:
        logger.warning("Cannot get Kafka producer: %s — skipping publish", exc)
        stats["failed"] = len(golden_records)
        return stats

    config = get_resolver_config()

    for record in golden_records:
        try:
            payload_bytes, key = build_resolved_event(record, trigger=trigger)

            if record.quality_flags.quarantined:
                topic = config.quarantine_topic
                stats["quarantined"] += 1
            else:
                topic = config.resolved_topic
                stats["resolved"] += 1

            producer.publish(
                topic=topic,
                key=key,
                value=payload_bytes,
            )

            logger.info(
                "Published golden record: entity_id=%s topic=%s confidence=%.3f",
                record.entity_id, topic, record.confidence_score,
            )

        except Exception as exc:
            logger.error("Failed to publish golden record %s: %s", record.entity_id, exc)
            stats["failed"] += 1

    return stats
