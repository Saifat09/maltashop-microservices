"""Order's side of the place-order saga (choreography).

Order starts the saga by emitting OrderPlaced and then reacts to what
Inventory and Payment report back. Each reaction is one local transaction:
de-duplicate, change state, stage the follow-up event in the outbox.
"""

from __future__ import annotations

import logging
from typing import Any

from maltashop_chassis.messaging import PermanentError, add_event, mark_processed
from sqlalchemy.ext.asyncio import async_sessionmaker

from .domain import InvalidTransition, OrderStatus, next_status
from .models import Order, Outbox, ProcessedMessage

log = logging.getLogger("order.saga")

SAGA_EVENTS = ["StockReserved", "StockRejected", "PaymentAuthorised", "PaymentFailed"]

# What Order announces once the saga reaches an outcome.
OUTCOME_EVENTS = {
    OrderStatus.CONFIRMED: "OrderConfirmed",
    OrderStatus.REJECTED: "OrderRejected",
    OrderStatus.CANCELLED: "OrderCancelled",   # triggers Inventory's compensation
}


def order_placed_payload(order: Order, customer_email: str, payment_token: str) -> dict[str, Any]:
    """The OrderPlaced v1 payload. Its shape is pinned by consumer contracts
    in /contracts/order-placed - change it only in a backwards-compatible way."""
    return {
        "order_id": order.id,
        "customer_id": order.customer_id,
        "customer_email": customer_email,
        "payment_token": payment_token,
        "total_cents": order.total_cents,
        "currency": order.currency,
        "lines": [{"sku": ln.sku, "quantity": ln.quantity, "unit_price_cents": ln.unit_price_cents}
                  for ln in order.lines],
    }


def make_saga_handler(session_factory: async_sessionmaker):
    async def handle(envelope: dict[str, Any]) -> None:
        event_type = envelope.get("event_type")
        payload = envelope.get("payload") or {}
        order_id = payload.get("order_id")
        if event_type not in SAGA_EVENTS or not order_id:
            raise PermanentError(f"unexpected message {event_type!r}")

        async with session_factory() as session, session.begin():
            if not await mark_processed(session, ProcessedMessage, envelope):
                return  # duplicate delivery - already applied
            order = await session.get(Order, order_id, with_for_update=True)
            if order is None:
                raise PermanentError(f"unknown order {order_id}")
            try:
                new_status = next_status(OrderStatus(order.status), event_type)
            except InvalidTransition as exc:
                # e.g. a late StockReserved after the order is already CONFIRMED.
                log.warning(str(exc), extra={"event": "saga.ignored", "order_id": order_id})
                return
            old_status, order.status = order.status, new_status
            if "reason" in payload:
                order.failure_reason = payload["reason"]
            log.info("order %s -> %s", old_status, new_status,
                     extra={"event": "order.status_changed", "order_id": order_id,
                            "from": old_status, "to": new_status, "trigger": event_type})
            if new_status in OUTCOME_EVENTS:
                add_event(session, Outbox, OUTCOME_EVENTS[new_status], {
                    "order_id": order.id,
                    "customer_id": order.customer_id,
                    "status": new_status,
                    "reason": order.failure_reason,
                })

    return handle
