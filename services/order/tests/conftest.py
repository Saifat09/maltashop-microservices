import os

import pytest
from fastapi.testclient import TestClient
from maltashop_chassis.messaging import FakeBus

from app.config import Settings
from app.main import create_app


@pytest.fixture
def settings(tmp_path):
    # Component tests run against a throwaway SQLite file and no broker, so
    # they need no containers and finish in seconds.
    url = os.getenv("TEST_DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path}/order-test.db")
    return Settings(database_url=url, broker_enabled=False, log_level="WARNING")


@pytest.fixture
def client(settings):
    app = create_app(settings, bus=FakeBus())
    with TestClient(app) as c:
        yield c


@pytest.fixture
def order_body():
    return {
        "customer_id": "cust-001",
        "customer_email": "maria.borg@example.com.mt",
        "payment_token": "tok_visa_ok",
        "items": [
            {"sku": "TV-55-OLED", "quantity": 1, "unit_price_cents": 129900},
            {"sku": "KETTLE-1L", "quantity": 2, "unit_price_cents": 2499},
        ],
    }
