"""Payment service: authorises card payments with the external provider once
stock is reserved. Card numbers never enter MaltaShop - only provider tokens."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Response, status
from maltashop_chassis.db import (
    OutboxMixin,
    ProcessedMessageMixin,
    create_schema,
    make_engine,
    make_session_factory,
    ping,
    utcnow,
)
from maltashop_chassis.http import CorrelationIdMiddleware
from maltashop_chassis.logging import configure_logging
from maltashop_chassis.messaging import EventBus, PermanentError, add_event, mark_processed, relay_outbox
from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SERVICE = os.getenv("SERVICE_NAME", "payment-service")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./payment.db")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
BROKER_ENABLED = os.getenv("BROKER_ENABLED", "true").lower() == "true"
PROVIDER_TIMEOUT_S = float(os.getenv("PROVIDER_TIMEOUT_SECONDS", "3"))

log = logging.getLogger("payment")


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    order_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    amount_cents: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(3))
    payment_token: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(12))            # PENDING | AUTHORISED | DECLINED
    provider_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class Outbox(OutboxMixin, Base):
    pass


class ProcessedMessage(ProcessedMessageMixin, Base):
    pass


class FakePaymentProvider:
    """Stands in for the external PSP. Like real providers it honours an
    idempotency key, so a retried authorisation never charges twice."""

    def __init__(self) -> None:
        self._results: dict[str, tuple[bool, str]] = {}

    async def authorise(self, idempotency_key: str, token: str, amount_cents: int) -> tuple[bool, str]:
        if idempotency_key in self._results:
            return self._results[idempotency_key]
        await asyncio.sleep(0.05)                               # network round-trip
        approved = token != "tok_decline"
        result = (approved, f"psp_{uuid.uuid4().hex[:12]}" if approved else "card_declined")
        self._results[idempotency_key] = result
        return result


def make_handler(session_factory, provider: FakePaymentProvider):
    async def handle(envelope: dict[str, Any]) -> None:
        kind, payload = envelope.get("event_type"), envelope.get("payload") or {}
        if kind == "OrderPlaced":
            async with session_factory() as s, s.begin():
                if not await mark_processed(s, ProcessedMessage, envelope):
                    return
                try:
                    s.add(Payment(order_id=payload["order_id"], amount_cents=payload["total_cents"],
                                  currency=payload["currency"], payment_token=payload["payment_token"],
                                  status="PENDING"))
                except KeyError as exc:
                    raise PermanentError(f"contract violation, missing {exc}") from exc
            log.info("payment intent recorded", extra={"event": "payment.pending", "order_id": payload["order_id"]})
            return

        if kind != "StockReserved":
            raise PermanentError(f"unexpected {kind!r}")

        order_id = payload["order_id"]
        async with session_factory() as s, s.begin():
            if not await mark_processed(s, ProcessedMessage, envelope):
                return
            payment = await s.get(Payment, order_id, with_for_update=True)
            if payment is None:
                # OrderPlaced not processed yet: fail -> broker redelivers (bounded by x-delivery-limit).
                raise RuntimeError(f"no payment intent yet for {order_id}")
            if payment.status != "PENDING":
                return
            # Timeout so a slow provider cannot hold the consumer forever;
            # on timeout the message is retried and the idempotency key
            # (order_id) stops the provider charging twice.
            approved, ref = await asyncio.wait_for(
                provider.authorise(order_id, payment.payment_token, payment.amount_cents), PROVIDER_TIMEOUT_S)
            if approved:
                payment.status, payment.provider_ref = "AUTHORISED", ref
                add_event(s, Outbox, "PaymentAuthorised", {"order_id": order_id, "provider_ref": ref})
            else:
                payment.status = "DECLINED"
                add_event(s, Outbox, "PaymentFailed", {"order_id": order_id, "reason": ref})
            log.info("payment %s", payment.status.lower(),
                     extra={"event": f"payment.{payment.status.lower()}", "order_id": order_id,
                            "amount_cents": payment.amount_cents})

    return handle


def create_app(database_url: str = DATABASE_URL, broker_enabled: bool = BROKER_ENABLED) -> FastAPI:
    configure_logging(SERVICE)
    engine = make_engine(database_url)
    session_factory = make_session_factory(engine)
    provider = FakePaymentProvider()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await create_schema(engine, Base)
        tasks, bus = [], None
        if broker_enabled:
            bus = EventBus(RABBITMQ_URL, SERVICE)
            await bus.connect()
            await bus.consume("payment-service.orders", ["OrderPlaced", "StockReserved"],
                              make_handler(session_factory, provider))
            tasks.append(asyncio.create_task(relay_outbox(session_factory, Outbox, bus)))
        app.state.bus = bus
        log.info("payment service started", extra={"event": "service.started"})
        yield
        for t in tasks:
            t.cancel()
        if bus:
            await bus.close()
        await engine.dispose()

    app = FastAPI(title="MaltaShop Payment Service", version="0.1.0", lifespan=lifespan)
    app.add_middleware(CorrelationIdMiddleware)
    app.state.session_factory = session_factory
    app.state.provider = provider

    @app.get("/payments/{order_id}")
    async def get_payment(order_id: str) -> dict:
        async with session_factory() as s:
            p = await s.get(Payment, order_id)
            if p is None:
                raise HTTPException(404, "no payment for order")
            # Never return the token.
            return {"order_id": p.order_id, "status": p.status, "amount_cents": p.amount_cents,
                    "currency": p.currency, "provider_ref": p.provider_ref}

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
