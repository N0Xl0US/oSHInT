"""
kafka/producer.py — Singleton Kafka producer with health checks.

Production rewrite: accepts ``Settings`` via DI, provides ``health_check()``,
context-manager support, and delivery metrics.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from confluent_kafka import KafkaException, Producer

from maigret.config import Settings, get_settings

logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    Managed wrapper around ``confluent_kafka.Producer``.

    Lazily initialised; thread-safe singleton via module-level instance.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._producer: Optional[Producer] = None
        self._messages_sent: int = 0
        self._bytes_sent: int = 0
        self._errors: int = 0

    # ── Lifecycle ────────────────────────────────────────────────────────

    def _ensure_producer(self) -> Producer:
        """Create the confluent-kafka Producer if it doesn't exist yet."""
        if self._producer is None:
            config = {
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "acks": self._settings.kafka_acks,
                "retries": self._settings.kafka_retries,
                "retry.backoff.ms": self._settings.kafka_retry_backoff_ms,
                "linger.ms": self._settings.kafka_linger_ms,
                "batch.num.messages": self._settings.kafka_batch_num_messages,
                "compression.type": self._settings.kafka_compression,
                "enable.idempotence": self._settings.kafka_idempotence,
            }
            logger.info("Initialising Kafka producer → %s", self._settings.kafka_bootstrap_servers)
            try:
                self._producer = Producer(config)
                logger.info("Kafka producer ready")
            except KafkaException as exc:
                logger.error("Failed to create Kafka producer: %s", exc)
                raise
        return self._producer

    def close(self, timeout: float | None = None) -> None:
        """Flush and release the producer."""
        if self._producer is not None:
            timeout = timeout or self._settings.kafka_flush_timeout
            remaining = self._producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning("%d Kafka messages still undelivered after flush", remaining)
            self._producer = None
            logger.info("Kafka producer closed")

    def __enter__(self) -> "KafkaProducer":
        self._ensure_producer()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ── Health ───────────────────────────────────────────────────────────

    def health_check(self, timeout: float = 5.0) -> bool:
        """
        Check broker connectivity by calling ``list_topics()``.

        Returns True if at least one broker responds within ``timeout``.
        """
        try:
            producer = self._ensure_producer()
            metadata = producer.list_topics(timeout=timeout)
            return len(metadata.brokers) > 0
        except Exception as exc:
            logger.warning("Kafka health check failed: %s", exc)
            return False

    # ── Publish ──────────────────────────────────────────────────────────

    def _delivery_report(self, err: object, msg: object) -> None:
        """Callback fired after each message is acknowledged."""
        if err:
            self._errors += 1
            logger.error(
                "Kafka delivery FAILED | topic=%s partition=%s | %s",
                msg.topic(), msg.partition(), err,
            )
        else:
            logger.debug(
                "Kafka delivery OK | topic=%s partition=%s offset=%s",
                msg.topic(), msg.partition(), msg.offset(),
            )

    def publish(
        self,
        payload_bytes: bytes,
        key: Optional[str] = None,
        topic: str | None = None,
    ) -> None:
        """
        Publish a pre-serialised message to Kafka.

        Test-trigger events are automatically routed to the ``.test.`` topic.
        """
        explicit_topic = topic is not None
        topic = topic or self._settings.kafka_topic

        # Route test events to test topic
        if not explicit_topic:
            try:
                payload_dict = json.loads(payload_bytes)
                if payload_dict.get("query", {}).get("trigger") == "test":
                    topic = topic.replace(".v1", ".test.v1") if ".v1" in topic else topic + ".test"
            except Exception:
                pass

        producer = self._ensure_producer()

        try:
            producer.produce(
                topic=topic,
                value=payload_bytes,
                key=key.encode() if key else None,
                on_delivery=self._delivery_report,
            )
            producer.poll(0)
            self._messages_sent += 1
            self._bytes_sent += len(payload_bytes)
        except BufferError:
            logger.warning("Kafka producer queue full — flushing before retry")
            producer.flush(timeout=10)
            producer.produce(
                topic=topic,
                value=payload_bytes,
                key=key.encode() if key else None,
                on_delivery=self._delivery_report,
            )
            producer.poll(0)
            self._messages_sent += 1
            self._bytes_sent += len(payload_bytes)
        except KafkaException as exc:
            self._errors += 1
            logger.error("Kafka produce error: %s", exc)
            raise

    # ── Metrics ──────────────────────────────────────────────────────────

    @property
    def messages_sent(self) -> int:
        return self._messages_sent

    @property
    def bytes_sent(self) -> int:
        return self._bytes_sent

    @property
    def errors(self) -> int:
        return self._errors

    def flush(self, timeout: float | None = None) -> None:
        """Block until all queued messages are delivered."""
        if self._producer is not None:
            timeout = timeout or self._settings.kafka_flush_timeout
            remaining = self._producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning("%d Kafka messages still undelivered after flush", remaining)


# ── Module-level singleton ──────────────────────────────────────────────────

_instance: Optional[KafkaProducer] = None


def get_kafka_producer(settings: Settings | None = None) -> KafkaProducer:
    """Return the module-level singleton producer."""
    global _instance
    if _instance is None:
        _instance = KafkaProducer(settings)
    return _instance


# ── Convenience functions (backward-compatible) ──────────────────────────────


def publish(payload_bytes: bytes, key: Optional[str] = None, topic: str | None = None) -> None:
    """Convenience: publish via the singleton."""
    get_kafka_producer().publish(payload_bytes, key=key, topic=topic)


def flush(timeout: float | None = None) -> None:
    """Convenience: flush the singleton."""
    get_kafka_producer().flush(timeout=timeout)