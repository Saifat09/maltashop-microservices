"""Event bus on RabbitMQ (topic exchange) plus the outbox relay.

Delivery guarantee: at-least-once. The outbox makes publishing reliable, and
consumers are idempotent (see ProcessedMessageMixin), so a duplicate delivery
never applies a change twice.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .logging import correlation_id_var, get_correlation_id

log = logging.getLogger("messaging")

EXCHANGE = "maltashop.events"
DEAD_LETTER_EXCHANGE = "maltashop.dlx"

Handler = Callable[[dict[str, Any]], Awaitable[None]]


class PermanentError(Exception):
    """The message can never succeed (e.g. it breaks the contract).
    It is dead-lettered at once instead of being retried."""


def make_envelope(event_type: str, payload: dict[str, Any], *, version: int = 1,
                  event_id: str | None = None, correlation_id: str | None = None) -> dict[str, Any]:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "version": version,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id or get_correlation_id(),
        "payload": payload,
    }


def add_event(session: AsyncSession, outbox_model: type, event_type: str, payload: dict[str, Any]) -> dict:
    """Stage an event in the outbox inside the caller's open transaction."""
    envelope = make_envelope(event_type, payload)
    session.add(outbox_model(
        event_id=envelope["event_id"],
        event_type=event_type,
        payload=json.dumps(envelope),
        correlation_id=envelope["correlation_id"],
    ))
    return envelope


class EventBus:
    def __init__(self, url: str, service: str) -> None:
        self.url = url
        self.service = service
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None

    @property
    def connected(self) -> bool:
        return self._connection is not None and not self._connection.is_closed

    async def connect(self, attempts: int = 30) -> None:
        for attempt in range(1, attempts + 1):
            try:
                self._connection = await aio_pika.connect_robust(self.url, client_properties={"connection_name": self.service})
                break
            except Exception as exc:  # noqa: BLE001 - broker may still be booting
                if attempt == attempts:
                    raise
                log.warning("broker not ready (%s), retrying", type(exc).__name__,
                            extra={"event": "broker.retry", "attempt": attempt})
                await asyncio.sleep(2)
        self._channel = await self._connection.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=10)
        self._exchange = await self._channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
        dlx = await self._channel.declare_exchange(DEAD_LETTER_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True)
        dlq = await self._channel.declare_queue("maltashop.dead-letter", durable=True)
        await dlq.bind(dlx)
        log.info("connected to broker", extra={"event": "broker.connected"})

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()

    async def publish(self, envelope: dict[str, Any]) -> None:
        assert self._exchange is not None, "call connect() first"
        message = aio_pika.Message(
            body=json.dumps(envelope).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=envelope["event_id"],
            correlation_id=envelope["correlation_id"],
            type=envelope["event_type"],
            app_id=self.service,
            headers={"x-correlation-id": envelope["correlation_id"], "x-event-version": envelope["version"]},
        )
        # With publisher confirms this await returns only once the broker
        # has taken responsibility for the message.
        await self._exchange.publish(message, routing_key=envelope["event_type"])

    async def consume(self, queue_name: str, routing_keys: list[str], handler: Handler) -> None:
        assert self._channel is not None and self._exchange is not None
        queue = await self._channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-queue-type": "quorum",          # replicated, survives a broker node loss
                "x-delivery-limit": 5,             # poison-message guard
                "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
            },
        )
        for key in routing_keys:
            await queue.bind(self._exchange, routing_key=key)

        async def on_message(message: AbstractIncomingMessage) -> None:
            cid = (message.headers or {}).get("x-correlation-id") or message.correlation_id or "-"
            token = correlation_id_var.set(str(cid))
            try:
                envelope = json.loads(message.body)
                log.info("consumed %s", envelope.get("event_type"),
                         extra={"event": "message.consumed", "event_type": envelope.get("event_type"),
                                "event_id": envelope.get("event_id"), "queue": queue_name})
                await handler(envelope)
                await message.ack()
            except PermanentError as exc:
                log.error("rejecting message: %s", exc, extra={"event": "message.dead_lettered"})
                await message.reject(requeue=False)
            except Exception:  # noqa: BLE001 - transient: retry up to the delivery limit
                log.exception("handler failed, message will be redelivered", extra={"event": "message.retry"})
                await asyncio.sleep(1)
                await message.nack(requeue=True)
            finally:
                correlation_id_var.reset(token)

        await queue.consume(on_message)
        log.info("consuming %s", queue_name, extra={"event": "broker.consuming", "routing_keys": routing_keys})


async def relay_outbox(session_factory: async_sessionmaker, outbox_model: type, bus: EventBus,
                       interval: float = 0.5, batch_size: int = 50) -> None:
    """Poll the outbox and publish unsent events, oldest first.

    FOR UPDATE SKIP LOCKED lets several replicas of a service run the relay
    without publishing the same row twice at the same moment. If the process
    dies after publishing but before marking the row, the event is published
    again on restart - which is why consumers must be idempotent.
    """
    while True:
        try:
            async with session_factory() as session, session.begin():
                rows = (await session.execute(
                    select(outbox_model)
                    .where(outbox_model.published_at.is_(None))
                    .order_by(outbox_model.created_at)
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                )).scalars().all()
                for row in rows:
                    envelope = json.loads(row.payload)
                    token = correlation_id_var.set(row.correlation_id)
                    try:
                        await bus.publish(envelope)
                        row.published_at = datetime.now(timezone.utc)
                        log.info("published %s", row.event_type,
                                 extra={"event": "message.published", "event_type": row.event_type,
                                        "event_id": row.event_id})
                    finally:
                        correlation_id_var.reset(token)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - keep relaying after a broker blip
            log.exception("outbox relay error", extra={"event": "outbox.error"})
        await asyncio.sleep(interval)


async def mark_processed(session: AsyncSession, processed_model: type, envelope: dict[str, Any]) -> bool:
    """Return False if this event_id was already handled (duplicate delivery).
    Must be called inside the same transaction as the state change."""
    if await session.get(processed_model, envelope["event_id"]) is not None:
        log.info("duplicate event ignored", extra={"event": "message.duplicate", "event_id": envelope["event_id"]})
        return False
    session.add(processed_model(event_id=envelope["event_id"], event_type=envelope["event_type"]))
    return True


class FakeBus:
    """In-memory stand-in used by component tests."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []
        self.connected = True

    async def publish(self, envelope: dict[str, Any]) -> None:
        self.published.append(envelope)
