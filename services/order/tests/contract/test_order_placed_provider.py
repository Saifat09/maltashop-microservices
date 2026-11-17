"""Provider-side contract verification for OrderPlaced v1.

Order (provider) must satisfy every consumer's published expectations in
/contracts/order-placed/*.schema.json. The event under test is produced by
the real POST /orders code path, not hand-written, so the test catches a
field being renamed or dropped anywhere in that path.
"""

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from sqlalchemy import select

from app.models import Outbox

CONTRACTS = Path(__file__).resolve().parents[4] / "contracts" / "order-placed"
CONSUMER_SCHEMAS = sorted(CONTRACTS.glob("*.schema.json"))


def test_contracts_exist():
    names = {p.name.split(".")[0] for p in CONSUMER_SCHEMAS}
    assert {"inventory-service", "payment-service", "notification-service"} <= names


@pytest.mark.parametrize("schema_path", CONSUMER_SCHEMAS, ids=lambda p: p.name.split(".")[0])
def test_order_placed_satisfies_consumer_contract(client, order_body, schema_path):
    client.post("/orders", json=order_body)

    async def read():
        async with client.app.state.session_factory() as s:
            return (await s.execute(select(Outbox))).scalars().one()

    event = json.loads(client.portal.call(read).payload)
    schema = json.loads(schema_path.read_text())
    errors = sorted(Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)
                    .iter_errors(event), key=str)
    assert not errors, f"{schema_path.name} broken: " + "; ".join(e.message for e in errors)
