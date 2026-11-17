"""Order service: REST API that starts the place-order saga."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Response, status
from maltashop_chassis.db import create_schema, make_engine, make_session_factory, ping
from maltashop_chassis.http import CorrelationIdMiddleware
from maltashop_chassis.logging import configure_logging, get_correlation_id
from maltashop_chassis.messaging import EventBus, add_event, relay_outbox
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from .config import Settings
from .domain import OrderStatus, order_total_cents
from .handlers import SAGA_EVENTS, make_saga_handler, order_placed_payload
from .models import Base, Order, OrderLine, Outbox
from .schemas import OrderLineOut, OrderOut, PlaceOrderRequest

log = logging.getLogger("order.api")


def to_out(order: Order) -> OrderOut:
    return OrderOut(
        order_id=order.id, status=order.status, total_cents=order.total_cents,
        currency=order.currency, failure_reason=order.failure_reason,
        lines=[OrderLineOut(sku=ln.sku, quantity=ln.quantity, unit_price_cents=ln.unit_price_cents)
               for ln in order.lines],
    )


def create_app(settings: Settings | None = None, bus=None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.service_name, settings.log_level)
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await create_schema(engine, Base)
        tasks: list[asyncio.Task] = []
        nonlocal bus
        if settings.broker_enabled:
            bus = bus or EventBus(settings.rabbitmq_url, settings.service_name)
            await bus.connect()
            await bus.consume("order-service.saga", SAGA_EVENTS, make_saga_handler(session_factory))
            tasks.append(asyncio.create_task(
                relay_outbox(session_factory, Outbox, bus, settings.outbox_poll_interval)))
        app.state.bus = bus
        log.info("order service started", extra={"event": "service.started"})
        yield
        for t in tasks:
            t.cancel()
        if bus is not None and hasattr(bus, "close"):
            await bus.close()
        await engine.dispose()

    app = FastAPI(title="MaltaShop Order Service", version="0.1.0", lifespan=lifespan)
    app.add_middleware(CorrelationIdMiddleware)
    app.state.session_factory = session_factory

    @app.post("/orders", response_model=OrderOut, status_code=status.HTTP_202_ACCEPTED)
    async def place_order(body: PlaceOrderRequest, response: Response,
                          idempotency_key: str | None = Header(default=None, max_length=64)) -> OrderOut:
        """Accept an order and start the saga. Returns 202 because the outcome
        (CONFIRMED / REJECTED / CANCELLED) is decided asynchronously."""
        async with session_factory() as session:
            try:
                async with session.begin():
                    if idempotency_key:
                        existing = (await session.execute(
                            select(Order).where(Order.idempotency_key == idempotency_key))).scalar_one_or_none()
                        if existing:
                            response.status_code = status.HTTP_200_OK
                            log.info("idempotent replay", extra={"event": "order.replayed", "order_id": existing.id})
                            return to_out(existing)

                    order = Order(
                        customer_id=body.customer_id,
                        status=OrderStatus.PENDING,
                        total_cents=order_total_cents([(i.quantity, i.unit_price_cents) for i in body.items]),
                        idempotency_key=idempotency_key,
                        correlation_id=get_correlation_id(),
                        lines=[OrderLine(sku=i.sku, quantity=i.quantity, unit_price_cents=i.unit_price_cents)
                               for i in body.items],
                    )
                    session.add(order)
                    await session.flush()
                    # Same local transaction as the order row: both are saved or neither is.
                    add_event(session, Outbox, "OrderPlaced",
                              order_placed_payload(order, str(body.customer_email), body.payment_token))
            except IntegrityError:
                # Two concurrent requests carrying the same Idempotency-Key.
                raise HTTPException(status.HTTP_409_CONFLICT, "duplicate request in progress") from None

        log.info("order placed", extra={"event": "order.placed", "order_id": order.id,
                                        "total_cents": order.total_cents})
        return to_out(order)

    @app.get("/orders/{order_id}", response_model=OrderOut)
    async def get_order(order_id: str) -> OrderOut:
        async with session_factory() as session:
            order = await session.get(Order, order_id)
            if order is None:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "order not found")
            return to_out(order)

    @app.get("/health/live")
    async def live() -> dict:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready(response: Response) -> dict:
        db_ok = await ping(engine)
        broker_ok = (not settings.broker_enabled) or bool(app.state.bus and app.state.bus.connected)
        if not (db_ok and broker_ok):
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"database": db_ok, "broker": broker_ok}

    return app


app = create_app()
