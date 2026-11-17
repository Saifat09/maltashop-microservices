"""Twelve-factor configuration: everything comes from environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    service_name: str = os.getenv("SERVICE_NAME", "order-service")
    database_url: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./order.db")
    rabbitmq_url: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    broker_enabled: bool = os.getenv("BROKER_ENABLED", "true").lower() == "true"
    outbox_poll_interval: float = float(os.getenv("OUTBOX_POLL_INTERVAL", "0.5"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
