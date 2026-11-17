"""Database helpers shared by services. Each service still owns its own
database and schema; only the plumbing is shared."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import DateTime, String, Text, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

log = logging.getLogger("db")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def make_engine(url: str) -> AsyncEngine:
    kwargs = {"pool_pre_ping": True}
    if url.startswith("postgresql"):
        kwargs.update(pool_size=5, max_overflow=5)
    return create_async_engine(url, **kwargs)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_schema(engine: AsyncEngine, base: type[DeclarativeBase], attempts: int = 30) -> None:
    """Create tables, retrying while the database container is still starting.
    (A real deployment would run Alembic migrations as a pipeline step.)"""
    for attempt in range(1, attempts + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(base.metadata.create_all)
            return
        except Exception as exc:  # noqa: BLE001 - startup retry
            if attempt == attempts:
                raise
            log.warning("database not ready (%s), retrying", type(exc).__name__,
                        extra={"event": "db.retry", "attempt": attempt})
            await asyncio.sleep(2)


async def ping(engine: AsyncEngine) -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


class OutboxMixin:
    """Transactional outbox row. Written in the SAME local transaction as the
    business change, then published to the broker by the relay."""

    __tablename__ = "outbox"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[str] = mapped_column(Text)
    correlation_id: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class ProcessedMessageMixin:
    """Inbox / de-duplication table. The primary key on event_id makes a
    redelivered message a no-op (idempotent consumer pattern)."""

    __tablename__ = "processed_messages"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64))
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
