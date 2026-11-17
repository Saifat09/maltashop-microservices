"""Order domain rules, kept free of frameworks so they are cheap to unit test."""

from __future__ import annotations

from enum import StrEnum


class OrderStatus(StrEnum):
    PENDING = "PENDING"                 # created, saga started
    STOCK_RESERVED = "STOCK_RESERVED"   # Inventory has held the items
    CONFIRMED = "CONFIRMED"             # payment authorised - saga complete
    REJECTED = "REJECTED"               # out of stock - nothing to undo
    CANCELLED = "CANCELLED"             # payment failed - stock released by compensation


# Which saga event may move an order from which state to which state.
# PaymentAuthorised is also accepted from PENDING because, with two
# different publishers, it can overtake StockReserved on the Order queue.
TRANSITIONS: dict[str, dict[OrderStatus, OrderStatus]] = {
    "StockReserved": {OrderStatus.PENDING: OrderStatus.STOCK_RESERVED},
    "StockRejected": {OrderStatus.PENDING: OrderStatus.REJECTED},
    "PaymentAuthorised": {
        OrderStatus.PENDING: OrderStatus.CONFIRMED,
        OrderStatus.STOCK_RESERVED: OrderStatus.CONFIRMED,
    },
    "PaymentFailed": {
        OrderStatus.PENDING: OrderStatus.CANCELLED,
        OrderStatus.STOCK_RESERVED: OrderStatus.CANCELLED,
    },
}

TERMINAL = {OrderStatus.CONFIRMED, OrderStatus.REJECTED, OrderStatus.CANCELLED}


class InvalidTransition(Exception):
    pass


def next_status(current: OrderStatus, event_type: str) -> OrderStatus:
    allowed = TRANSITIONS.get(event_type, {})
    if current not in allowed:
        raise InvalidTransition(f"{event_type} not allowed when order is {current}")
    return allowed[current]


def order_total_cents(lines: list[tuple[int, int]]) -> int:
    """lines = [(quantity, unit_price_cents), ...]"""
    return sum(q * p for q, p in lines)
