import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from maltashop_chassis.messaging import PermanentError, make_envelope

from app.main import create_app, make_handler

CONTRACT = Path(__file__).resolve().parents[3] / "contracts" / "order-placed" / "notification-service.schema.json"


@pytest.fixture
def client(tmp_path):
    app = create_app(f"sqlite+aiosqlite:///{tmp_path}/notif.db", broker_enabled=False)
    with TestClient(app) as c:
        yield c


def run(client, env):
    client.portal.call(make_handler(client.app.state.session_factory), env)


def test_contract_example_creates_notification(client):
    example = json.loads(CONTRACT.read_text())["examples"][0]
    run(client, example)
    sent = client.get("/notifications", params={"order_id": "ord-contract-3"}).json()
    assert [n["trigger"] for n in sent] == ["OrderPlaced"]


def test_duplicate_event_sends_one_email(client):
    env = make_envelope("OrderConfirmed", {"order_id": "o-9"})
    run(client, env)
    run(client, env)
    assert len(client.get("/notifications", params={"order_id": "o-9"}).json()) == 1


def test_unknown_event_is_dead_lettered(client):
    with pytest.raises(PermanentError):
        run(client, make_envelope("SomethingOdd", {"order_id": "x"}))
