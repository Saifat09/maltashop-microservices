"""Component tests: the Order service as a black box over HTTP, with a real
database engine (SQLite) and the broker replaced by the outbox table."""

import asyncio
import json

from maltashop_chassis.messaging import make_envelope
from sqlalchemy import select

from app.handlers import make_saga_handler
from app.models import Order, Outbox


def _outbox(client):
    async def read():
        async with client.app.state.session_factory() as s:
            return (await s.execute(select(Outbox).order_by(Outbox.created_at))).scalars().all()
    return client.portal.call(read)


def test_place_order_returns_202_and_stages_order_placed(client, order_body):
    r = client.post("/orders", json=order_body, headers={"X-Correlation-ID": "test-cid-1"})
    assert r.status_code == 202
    data = r.json()
    assert data["status"] == "PENDING"
    assert data["total_cents"] == 129900 + 2 * 2499
    assert r.headers["X-Correlation-ID"] == "test-cid-1"

    rows = _outbox(client)
    assert [row.event_type for row in rows] == ["OrderPlaced"]
    envelope = json.loads(rows[0].payload)
    assert envelope["correlation_id"] == "test-cid-1"
    assert envelope["payload"]["order_id"] == data["order_id"]


def test_idempotency_key_prevents_double_orders(client, order_body):
    headers = {"Idempotency-Key": "checkout-abc-123"}
    first = client.post("/orders", json=order_body, headers=headers)
    second = client.post("/orders", json=order_body, headers=headers)
    assert first.status_code == 202
    assert second.status_code == 200
    assert first.json()["order_id"] == second.json()["order_id"]
    assert len(_outbox(client)) == 1   # only one OrderPlaced event


def test_invalid_order_is_rejected(client, order_body):
    order_body["items"][0]["quantity"] = 0
    assert client.post("/orders", json=order_body).status_code == 422


def test_unknown_order_returns_404(client):
    assert client.get("/orders/does-not-exist").status_code == 404


def test_saga_happy_path_and_duplicate_delivery(client, order_body):
    order_id = client.post("/orders", json=order_body).json()["order_id"]
    handle = make_saga_handler(client.app.state.session_factory)

    reserved = make_envelope("StockReserved", {"order_id": order_id})
    authorised = make_envelope("PaymentAuthorised", {"order_id": order_id})
    client.portal.call(handle, reserved)
    client.portal.call(handle, authorised)
    client.portal.call(handle, authorised)   # redelivered by the broker

    assert client.get(f"/orders/{order_id}").json()["status"] == "CONFIRMED"
    types = [row.event_type for row in _outbox(client)]
    assert types == ["OrderPlaced", "OrderConfirmed"]   # not two OrderConfirmed


def test_payment_failure_cancels_order_and_requests_compensation(client, order_body):
    order_id = client.post("/orders", json=order_body).json()["order_id"]
    handle = make_saga_handler(client.app.state.session_factory)
    client.portal.call(handle, make_envelope("StockReserved", {"order_id": order_id}))
    client.portal.call(handle, make_envelope("PaymentFailed", {"order_id": order_id, "reason": "card_declined"}))

    order = client.get(f"/orders/{order_id}").json()
    assert order["status"] == "CANCELLED"
    assert order["failure_reason"] == "card_declined"
    assert _outbox(client)[-1].event_type == "OrderCancelled"


def test_ready_probe_reports_database(client):
    r = client.get("/health/ready")
    assert r.status_code == 200
    assert r.json()["database"] is True


def test_order_model_has_no_card_data():
    # GDPR / PCI DSS guard: the Order table must never hold card numbers.
    columns = {c.name for c in Order.__table__.columns}
    assert not {"card_number", "pan", "cvv"} & columns
    assert asyncio.iscoroutinefunction(make_saga_handler(None))
