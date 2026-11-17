"""Notification service: tells customers what happened to their order.

It runs in its own process with its own database, so a fault here (like the
March memory leak) can no longer take checkout down with it. It only
listens to events; nothing in the order flow waits for it.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, Response, status
from maltashop_chassis.db import ProcessedMessageMixin, create_schema, make_engine, make_session_factory, ping, utcnow
from maltashop_chassis.http import CorrelationIdMiddleware
from maltashop_chassis.logging import configure_logging
from maltashop_chassis.messaging import EventBus, PermanentError, mark_processed
from sqlalchemy import DateTime, Integer, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SERVICE = os.getenv("SERVICE_NAME", "notification-service")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./notification.db")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
BROKER_ENABLED = os.getenv("BROKER_ENABLED", "true").lower() == "true"

log = logging.getLogger("notification")

TEMPLATES = {
    "OrderPlaced": "Thanks! We have received order {order_id} ({total}).",
    "OrderConfirmed": "Good news: order {order_id} is confirmed and will be dispatched soon.",
    "OrderRejected": "Sorry, an item in order {order_id} is out of stock. You have not been charged.",
    "OrderCancelled": "Order {order_id} was cancelled because the payment did not go through.",
}


class Base(DeclarativeBase):
    pass


class Recipient(Base):
    """Local copy of the contact address for an order, taken from OrderPlaced,
    so later events need not carry personal data (GDPR data minimisation)."""
    __tablename__ = "recipients"
    order_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    email: Mapped[str] = mapped_column(String(254))


class Notification(Base):
    __tablename__ = "notifications"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(36), index=True)
    trigger: Mapped[str] = mapped_column(String(32))
    channel: Mapped[str] = mapped_column(String(10), default="email")
    body: Mapped[str] = mapped_column(String(300))
    status: Mapped[str] = mapped_column(String(10), default="SENT")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ProcessedMessage(ProcessedMessageMixin, Base):
    pass


def make_handler(session_factory):
    async def handle(envelope: dict[str, Any]) -> None:
        kind, payload = envelope.get("event_type"), envelope.get("payload") or {}
        if kind not in TEMPLATES or "order_id" not in payload:
            raise PermanentError(f"unexpected {kind!r}")
        order_id = payload["order_id"]
        async with session_factory() as s, s.begin():
            if not await mark_processed(s, ProcessedMessage, envelope):
                return
            if kind == "OrderPlaced":
                s.add(Recipient(order_id=order_id, email=payload["customer_email"]))
            total = f"EUR {payload.get('total_cents', 0) / 100:.2f}"
            body = TEMPLATES[kind].format(order_id=order_id[:8], total=total)
            s.add(Notification(order_id=order_id, trigger=kind, body=body))
        # Stub for the email provider call. Only the order id is logged,
        # never the address, to keep personal data out of the log store.
        log.info("email sent", extra={"event": "notification.sent", "order_id": order_id, "trigger": kind})

    return handle


def create_app(database_url: str = DATABASE_URL, broker_enabled: bool = BROKER_ENABLED) -> FastAPI:
    configure_logging(SERVICE)
    engine = make_engine(database_url)
    session_factory = make_session_factory(engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await create_schema(engine, Base)
        bus = None
        if broker_enabled:
            bus = EventBus(RABBITMQ_URL, SERVICE)
            await bus.connect()
            await bus.consume("notification-service.orders", list(TEMPLATES), make_handler(session_factory))
        app.state.bus = bus
        log.info("notification service started", extra={"event": "service.started"})
        yield
        if bus:
            await bus.close()
        await engine.dispose()

    app = FastAPI(title="MaltaShop Notification Service", version="0.1.0", lifespan=lifespan)
    app.add_middleware(CorrelationIdMiddleware)
    app.state.session_factory = session_factory

    @app.get("/notifications")
    async def list_notifications(order_id: str) -> list[dict]:
        async with session_factory() as s:
            rows = (await s.execute(select(Notification).where(Notification.order_id == order_id)
                                    .order_by(Notification.id))).scalars()
            return [{"trigger": n.trigger, "channel": n.channel, "body": n.body, "status": n.status,
                     "created_at": n.created_at.isoformat()} for n in rows]

    @app.get("/health/live")
    async def live() -> dict:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready(response: Response) -> dict:
        db_ok = await ping(engine)
        broker_ok = (not broker_enabled) or bool(app.state.bus and app.state.bus.connected)
        if not (db_ok and broker_ok):
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"database": db_ok, "broker": broker_ok}

    return app


app = create_app()
