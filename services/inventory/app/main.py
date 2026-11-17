"""Inventory service: reserves stock for new orders and releases it when an
order is cancelled (the saga's compensating action)."""

from __future__ import annotations

import asyncio
import logging
import os
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
from sqlalchemy import DateTime, Integer, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SERVICE = os.getenv("SERVICE_NAME", "inventory-service")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./inventory.db")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
BROKER_ENABLED = os.getenv("BROKER_ENABLED", "true").lower() == "true"
SEED_STOCK = os.getenv("SEED_STOCK", "TV-55-OLED=5,KETTLE-1L=50,AIRFRYER-XL=0,SOFA-3S=2")

log = logging.getLogger("inventory")


class Base(DeclarativeBase):
    pass


class StockItem(Base):
    __tablename__ = "stock_items"
    sku: Mapped[str] = mapped_column(String(40), primary_key=True)
    available: Mapped[int] = mapped_column(Integer)
    reserved: Mapped[int] = mapped_column(Integer, default=0)


class Reservation(Base):
    __tablename__ = "reservations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(36), index=True)
    sku: Mapped[str] = mapped_column(String(40))
    quantity: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(10))           # HELD | RELEASED
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class Outbox(OutboxMixin, Base):
    pass


class ProcessedMessage(ProcessedMessageMixin, Base):
    pass


def parse_seed(spec: str) -> dict[str, int]:
    pairs = (p.split("=") for p in spec.split(",") if "=" in p)
    return {sku.strip(): int(qty) for sku, qty in pairs}


def make_handler(session_factory):
    async def reserve(session, envelope: dict[str, Any]) -> None:
        payload = envelope["payload"]
        order_id = payload["order_id"]
        wanted = {ln["sku"]: ln["quantity"] for ln in payload["lines"]}
        # Lock rows in a fixed order so two concurrent orders cannot deadlock.
        items = {i.sku: i for i in (await session.execute(
            select(StockItem).where(StockItem.sku.in_(sorted(wanted))).order_by(StockItem.sku).with_for_update()
        )).scalars()}
        short = [sku for sku, qty in wanted.items() if sku not in items or items[sku].available < qty]
        if short:
            # All-or-nothing: no partial reservation is ever made.
            log.info("insufficient stock", extra={"event": "stock.rejected", "order_id": order_id, "skus": short})
            add_event(session, Outbox, "StockRejected",
                      {"order_id": order_id, "reason": "out_of_stock", "skus": short})
            return
        for sku, qty in wanted.items():
            items[sku].available -= qty
            items[sku].reserved += qty
            session.add(Reservation(order_id=order_id, sku=sku, quantity=qty, status="HELD"))
        log.info("stock reserved", extra={"event": "stock.reserved", "order_id": order_id, "lines": wanted})
        add_event(session, Outbox, "StockReserved", {"order_id": order_id})

    async def release(session, envelope: dict[str, Any]) -> None:
        """Compensating action for a failed payment."""
        order_id = envelope["payload"]["order_id"]
        held = (await session.execute(
            select(Reservation).where(Reservation.order_id == order_id, Reservation.status == "HELD")
        )).scalars().all()
        for r in held:
            item = await session.get(StockItem, r.sku, with_for_update=True)
            item.available += r.quantity
            item.reserved -= r.quantity
            r.status = "RELEASED"
        log.info("stock released (compensation)",
                 extra={"event": "stock.released", "order_id": order_id, "reservations": len(held)})

    async def handle(envelope: dict[str, Any]) -> None:
        kind = envelope.get("event_type")
        if kind not in ("OrderPlaced", "OrderCancelled"):
            raise PermanentError(f"unexpected {kind!r}")
        try:
            async with session_factory() as session, session.begin():
                if not await mark_processed(session, ProcessedMessage, envelope):
                    return
                await (reserve if kind == "OrderPlaced" else release)(session, envelope)
        except KeyError as exc:
            raise PermanentError(f"contract violation, missing {exc}") from exc

    return handle


def create_app(database_url: str = DATABASE_URL, broker_enabled: bool = BROKER_ENABLED) -> FastAPI:
    configure_logging(SERVICE)
    engine = make_engine(database_url)
    session_factory = make_session_factory(engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await create_schema(engine, Base)
        async with session_factory() as s, s.begin():
            for sku, qty in parse_seed(SEED_STOCK).items():
                if await s.get(StockItem, sku) is None:
                    s.add(StockItem(sku=sku, available=qty, reserved=0))
        tasks, bus = [], None
        if broker_enabled:
            bus = EventBus(RABBITMQ_URL, SERVICE)
            await bus.connect()
            await bus.consume("inventory-service.orders", ["OrderPlaced", "OrderCancelled"],
                              make_handler(session_factory))
            tasks.append(asyncio.create_task(relay_outbox(session_factory, Outbox, bus)))
        app.state.bus = bus
        log.info("inventory service started", extra={"event": "service.started"})
        yield
        for t in tasks:
            t.cancel()
        if bus:
            await bus.close()
        await engine.dispose()

    app = FastAPI(title="MaltaShop Inventory Service", version="0.1.0", lifespan=lifespan)
    app.add_middleware(CorrelationIdMiddleware)
    app.state.session_factory = session_factory

    @app.get("/stock/{sku}")
    async def get_stock(sku: str) -> dict:
        async with session_factory() as s:
            item = await s.get(StockItem, sku)
            if item is None:
                raise HTTPException(404, "unknown sku")
            return {"sku": item.sku, "available": item.available, "reserved": item.reserved}

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
