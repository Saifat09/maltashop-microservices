#!/usr/bin/env bash
# Fault-isolation check for the March incident: with the Notification
# service stopped, checkout must still confirm orders; the missed events
# wait in Notification's queue and are delivered once it is back.
#
#   bash scripts/isolation_test.sh [suffix]
set -euo pipefail

CID="isolation-${1:-local}"
BODY='{"customer_id":"cust-002","customer_email":"joe.camilleri@example.com.mt","payment_token":"tok_visa_ok","items":[{"sku":"KETTLE-1L","quantity":1,"unit_price_cents":2499}]}'

echo "\$ docker compose stop notification-service"
docker compose stop notification-service 2>&1 | tail -n 1
echo "\$ curl -s -m 2 http://localhost:8004/health/live"
if curl -s -m 2 http://localhost:8004/health/live; then echo; else echo "curl: connection refused (Notification is down)"; fi

echo "\$ curl -s -X POST http://localhost:8001/orders -H 'X-Correlation-ID: $CID' -d '<order.json>'"
ORDER_ID=$(curl -sS -X POST http://localhost:8001/orders -H "Content-Type: application/json" \
  -H "X-Correlation-ID: $CID" -d "$BODY" | python3 -c "import sys, json; print(json.load(sys.stdin)['order_id'])")
echo "order_id=$ORDER_ID"

STATUS=PENDING
for _ in $(seq 1 30); do
  STATUS=$(curl -sS "http://localhost:8001/orders/$ORDER_ID" | python3 -c "import sys, json; print(json.load(sys.stdin)['status'])")
  [ "$STATUS" = "CONFIRMED" ] && break
  sleep 0.5
done
echo "\$ curl -s http://localhost:8001/orders/$ORDER_ID   ->   status: $STATUS"
[ "$STATUS" = "CONFIRMED" ] || { echo "checkout was affected by the Notification outage"; exit 1; }

sleep 6   # queue statistics are refreshed every few seconds
echo "\$ docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready | grep notification"
docker compose exec -T rabbitmq rabbitmqctl -q list_queues name messages_ready | grep notification || true

echo "\$ docker compose start notification-service"
docker compose start notification-service 2>&1 | tail -n 1
for _ in $(seq 1 60); do
  curl -sf -m 2 http://localhost:8004/health/ready > /dev/null && break
  sleep 1
done
sleep 3
TRIGGERS=$(curl -sS "http://localhost:8004/notifications?order_id=$ORDER_ID" \
  | python3 -c "import sys, json; print(', '.join(n['trigger'] for n in json.load(sys.stdin)))")
echo "\$ curl -s 'http://localhost:8004/notifications?order_id=$ORDER_ID'   ->   $TRIGGERS"
[ "$TRIGGERS" = "OrderPlaced, OrderConfirmed" ] || { echo "backlog was not delivered"; exit 1; }
echo "FAULT ISOLATION TEST PASSED"
