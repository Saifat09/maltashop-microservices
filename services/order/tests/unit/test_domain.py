"""Unit tests: pure domain rules, no I/O. Run first in the pipeline (seconds)."""

import pytest

from app.domain import InvalidTransition, OrderStatus, next_status, order_total_cents


def test_total_is_sum_of_lines():
    assert order_total_cents([(2, 1999), (1, 54900)]) == 58898


@pytest.mark.parametrize(
    ("current", "event", "expected"),
    [
        (OrderStatus.PENDING, "StockReserved", OrderStatus.STOCK_RESERVED),
        (OrderStatus.PENDING, "StockRejected", OrderStatus.REJECTED),
        (OrderStatus.STOCK_RESERVED, "PaymentAuthorised", OrderStatus.CONFIRMED),
        (OrderStatus.STOCK_RESERVED, "PaymentFailed", OrderStatus.CANCELLED),
        # PaymentAuthorised can overtake StockReserved on the queue
        (OrderStatus.PENDING, "PaymentAuthorised", OrderStatus.CONFIRMED),
    ],
)
def test_valid_saga_transitions(current, event, expected):
    assert next_status(current, event) == expected


@pytest.mark.parametrize(
    ("current", "event"),
    [
        (OrderStatus.CONFIRMED, "StockReserved"),     # late event after completion
        (OrderStatus.REJECTED, "PaymentAuthorised"),
        (OrderStatus.CANCELLED, "PaymentAuthorised"),
        (OrderStatus.PENDING, "SomethingElse"),
    ],
)
def test_invalid_transitions_are_refused(current, event):
    with pytest.raises(InvalidTransition):
        next_status(current, event)
