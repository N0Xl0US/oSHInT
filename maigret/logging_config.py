"""
logging_config.py — Structured logging setup.

Call ``setup_logging()`` once at app startup.
- ``json`` format: machine-parseable (Datadog / ELK / CloudWatch)
- ``human`` format: coloured, readable for local dev
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class _JSONFormatter(logging.Formatter):
    """Emit one JSON object per log line."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1]:
            payload["exception"] = self.formatException(record.exc_info)
        # Attach extras added via logger.info("msg", extra={...})
        for key in ("request_id", "username", "step"):
            val = getattr(record, key, None)
            if val is not None:
                payload[key] = val
        return json.dumps(payload, ensure_ascii=False)


_HUMAN_FMT = "%(asctime)s [%(levelname)-5s] %(name)s — %(message)s"


def setup_logging(level: str = "INFO", fmt: str = "human") -> None:
    """Configure the root logger. Safe to call multiple times."""
    root = logging.getLogger()
    # Avoid duplicate handlers on repeated calls
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)
    if fmt == "json":
        handler.setFormatter(_JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(_HUMAN_FMT))

    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Quiet noisy third-party loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
