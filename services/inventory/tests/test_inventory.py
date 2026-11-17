import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from maltashop_chassis.messaging import make_envelope
from sqlalchemy import select

from app.main import Outbox, create_app, make_handler

CONTRACT = Path(__file__).resolve().parents[3] / "contracts" / "order-placed" / "inventory-service.schema.json"


@pytest.fixture
def client(tmp_path):
    app = create_app(f"sqlite+aiosqlite:///{tmp_path}/inv.db", broker_enabled=False)
    with TestClient(app) as c:
        yield c


def run(client, envelope):
    client.portal.call(make_handler(client.app.state.session_factory), envelope)


def events(client):
    async def read():
        async with client.app.state.session_factory() as s:
            return [o.event_type for o in (await s.execute(select(Outbox).order_by(Outbox.created_at))).scalars()]
    return client.portal.call(read)


def placed(order_id, lines):
    return make_envelope("OrderPlaced", {"order_id": order_id, "lines": lines})


def test_consumer_contract_example_is_accepted(client):
    """Consumer side of the Order -> Inventory contract: the example message in
    our own published schema must be processable by our handler."""
    example = json.loads(CONTRACT.read_text())["examples"][0]
    run(client, example)
    assert events(client) == ["StockReserved"]


def test_reserve_then_release_restores_stock(client):
    run(client, placed("o-1", [{"sku": "TV-55-OLED", "quantity": 2}]))
    assert client.get("/stock/TV-55-OLED").json() == {"sku": "TV-55-OLED", "available": 3, "reserved": 2}
    run(client, make_envelope("OrderCancelled", {"order_id": "o-1"}))
    assert client.get("/stock/TV-55-OLED").json() == {"sku": "TV-55-OLED", "available": 5, "reserved": 0}


def test_out_of_stock_is_all_or_nothing(client):
    run(client, placed("o-2", [{"sku": "KETTLE-1L", "quantity": 1}, {"sku": "AIRFRYER-XL", "quantity": 1}]))
    assert events(client) == ["StockRejected"]
    assert client.get("/stock/KETTLE-1L").json()["available"] == 50   # nothing held


def test_duplicate_delivery_reserves_once(client):
    env = placed("o-3", [{"sku": "SOFA-3S", "quantity": 1}])
    run(client, env)
    run(client, env)
    assert client.get("/stock/SOFA-3S").json()["available"] == 1
    assert events(client) == ["StockReserved"]


def test_compensation_is_idempotent(client):
    run(client, placed("o-4", [{"sku": "SOFA-3S", "quantity": 2}]))
    run(client, make_envelope("OrderCancelled", {"order_id": "o-4"}))
    run(client, make_envelope("OrderCancelled", {"order_id": "o-4"}))   # different event_id, same intent
    assert client.get("/stock/SOFA-3S").json() == {"sku": "SOFA-3S", "available": 2, "reserved": 0}
