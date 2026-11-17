"""Structured JSON logging with a mandatory correlation ID.

Every log line is one JSON object that follows the MaltaShop log schema:
timestamp, level, service, correlation_id, event, message, plus optional
context fields (order_id, sku, ...). The correlation ID is held in a
ContextVar so it follows a request or a message through async code without
being passed around by hand.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


def new_correlation_id() -> str:
    return str(uuid.uuid4())


def get_correlation_id() -> str:
    return correlation_id_var.get()


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": self.service,
            "correlation_id": correlation_id_var.get(),
            "event": getattr(record, "event", record.name),
            "message": record.getMessage(),
        }
        # Any extra={...} fields become top-level keys (order_id, sku, ...).
        for key, value in record.__dict__.items():
            if key not in _RESERVED and key not in entry and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(service: str, level: str | None = None) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel((level or os.getenv("LOG_LEVEL", "INFO")).upper())
    # Uvicorn's access log duplicates our request log; keep its errors only.
    logging.getLogger("uvicorn.access").disabled = True
    for noisy in ("aio_pika", "aiormq", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
