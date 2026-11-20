#!/usr/bin/env bash
# Places one order against the running stack and shows its journey:
# the REST request and response, the final order state, the notification
# records written by the second service, and the correlated log lines.
#
#   bash scripts/demo.sh [correlation-id]
set -euo pipefail

CID="${1:-demo-$(date +%s)}"
BODY='{"customer_id":"cust-001","customer_email":"maria.borg@example.com.mt","payment_token":"tok_visa_ok","items":[{"sku":"TV-55-OLED","quantity":1,"unit_price_cents":129900},{"sku":"KETTLE-1L","quantity":2,"unit_price_cents":2499}]}'

echo "\$ curl -i -X POST http://localhost:8001/orders -H 'X-Correlation-ID: $CID' -H 'Idempotency-Key: $CID' -d '<order.json>'"
RESPONSE=$(curl -sS -i -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" -H "X-Correlation-ID: $CID" -H "Idempotency-Key: $CID" -d "$BODY" | tr -d '\r')
echo "$RESPONSE"
ORDER_ID=$(echo "$RESPONSE" | tail -n 1 | python3 -c "import sys, json; print(json.load(sys.stdin)['order_id'])")
echo

sleep 3
echo "\$ curl -s http://localhost:8001/orders/$ORDER_ID"
curl -sS "http://localhost:8001/orders/$ORDER_ID" | python3 -m json.tool
echo
echo "\$ curl -s 'http://localhost:8004/notifications?order_id=$ORDER_ID'"
curl -sS "http://localhost:8004/notifications?order_id=$ORDER_ID" | python3 -m json.tool
echo
echo "\$ docker compose logs --no-log-prefix | python3 scripts/trace.py $CID"
docker compose logs --no-log-prefix | python3 scripts/trace.py "$CID"
echo
echo "\$ docker compose logs --no-log-prefix order-service notification-service | grep '\"$CID\"' | grep -E 'order.placed|consumed OrderPlaced'"
docker compose logs --no-log-prefix order-service notification-service \
  | grep "\"$CID\"" | grep -E '"event": "order.placed"|"message": "consumed OrderPlaced"' || true
