#!/usr/bin/env python3
"""End-to-end smoke test for the place-order saga.

Runs against a live system (docker compose up) using only the standard
library, so the CI runner needs nothing extra. Exits non-zero on failure.

    python scripts/smoke_test.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

ORDER = os.getenv("ORDER_URL", "http://localhost:8001")
INVENTORY = os.getenv("INVENTORY_URL", "http://localhost:8002")
NOTIFICATION = os.getenv("NOTIFICATION_URL", "http://localhost:8004")


def call(method: str, url: str, body: dict | None = None, headers: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, dict(resp.headers), json.loads(resp.read() or b"null")


def wait_ready(timeout: float = 90) -> None:
    deadline = time.time() + timeout
    for base in (ORDER, INVENTORY, NOTIFICATION):
        while True:
            try:
                if call("GET", f"{base}/health/ready")[0] == 200:
                    break
            except (urllib.error.URLError, ConnectionError):
                pass
            if time.time() > deadline:
                sys.exit(f"{base} never became ready")
            time.sleep(2)


def place(sku: str, qty: int, token: str) -> tuple[str, str]:
    cid = f"smoke-{uuid.uuid4().hex[:8]}"
    status, headers, body = call("POST", f"{ORDER}/orders", {
        "customer_id": "cust-smoke",
        "customer_email": "smoke.test@example.com.mt",
        "payment_token": token,
        "items": [{"sku": sku, "quantity": qty, "unit_price_cents": 2499}],
    }, {"X-Correlation-ID": cid, "Idempotency-Key": cid})
    assert status == 202, status
    assert headers.get("x-correlation-id", headers.get("X-Correlation-ID")) == cid
    print(f"POST /orders -> {status} order_id={body['order_id']} status={body['status']} correlation_id={cid}")
    return body["order_id"], cid


def wait_status(order_id: str, expected: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        order = call("GET", f"{ORDER}/orders/{order_id}")[2]
        if order["status"] == expected:
            print(f"GET /orders/{order_id[:8]}... -> {order['status']}")
            return order
        time.sleep(0.3)
    sys.exit(f"order {order_id} stuck in {order['status']}, expected {expected}")


def stock(sku: str) -> int:
    return call("GET", f"{INVENTORY}/stock/{sku}")[2]["available"]


def main() -> None:
    wait_ready()

    # 1. Happy path: reserve -> authorise -> confirm -> notify
    before = stock("KETTLE-1L")
    oid, _ = place("KETTLE-1L", 1, "tok_visa_ok")
    wait_status(oid, "CONFIRMED")
    assert stock("KETTLE-1L") == before - 1
    time.sleep(1)
    triggers = [n["trigger"] for n in call("GET", f"{NOTIFICATION}/notifications?order_id={oid}")[2]]
    assert triggers == ["OrderPlaced", "OrderConfirmed"], triggers
    print(f"notifications for {oid[:8]}...: {triggers}")

    # 2. Card declined: compensation must give the stock back
    before = stock("KETTLE-1L")
    oid, _ = place("KETTLE-1L", 2, "tok_decline")
    wait_status(oid, "CANCELLED")
    time.sleep(1)
    assert stock("KETTLE-1L") == before, "stock not released by compensation"
    print("declined payment -> CANCELLED, stock released")

    # 3. Out of stock: rejected before any payment is attempted
    oid, _ = place("AIRFRYER-XL", 1, "tok_visa_ok")
    wait_status(oid, "REJECTED")
    print("out of stock -> REJECTED")

    print("SMOKE TEST PASSED")


if __name__ == "__main__":
    main()
