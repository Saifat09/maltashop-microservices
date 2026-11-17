import pytest
from fastapi.testclient import TestClient
from maltashop_chassis.messaging import make_envelope

from app.main import create_app, make_handler


@pytest.fixture
def client(tmp_path):
    app = create_app(f"sqlite+aiosqlite:///{tmp_path}/pay.db", broker_enabled=False)
    with TestClient(app) as c:
        yield c


def run(client, env):
    handler = make_handler(client.app.state.session_factory, client.app.state.provider)
    client.portal.call(handler, env)


def placed(order_id, token):
    return make_envelope("OrderPlaced", {"order_id": order_id, "total_cents": 4998,
                                         "currency": "EUR", "payment_token": token})


def test_authorised_after_stock_reserved(client):
    run(client, placed("o-1", "tok_visa_ok"))
    assert client.get("/payments/o-1").json()["status"] == "PENDING"
    run(client, make_envelope("StockReserved", {"order_id": "o-1"}))
    body = client.get("/payments/o-1").json()
    assert body["status"] == "AUTHORISED"
    assert "payment_token" not in body


def test_declined_card(client):
    run(client, placed("o-2", "tok_decline"))
    run(client, make_envelope("StockReserved", {"order_id": "o-2"}))
    assert client.get("/payments/o-2").json()["status"] == "DECLINED"


def test_stock_reserved_before_intent_is_retried(client):
    with pytest.raises(RuntimeError):
        run(client, make_envelope("StockReserved", {"order_id": "o-3"}))
