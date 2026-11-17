# MaltaShop – "Place an Order" Microservices Prototype

A small, runnable prototype of the place-order flow for **MaltaShop Ltd**, built for
*Unit 5: Service-Oriented Architecture and Microservices Fundamentals*. It shows how
the order flow stays consistent across services without a shared database, using a
choreographed saga, the transactional outbox and idempotent consumers on RabbitMQ.

## What the system is

| Service | Port | Owns (own PostgreSQL) | Role in the saga |
|---|---|---|---|
| `order-service` | 8001 | orders, order lines, outbox | REST API `POST /orders`; starts the saga with `OrderPlaced`, ends it with `OrderConfirmed` / `OrderRejected` / `OrderCancelled` |
| `inventory-service` | 8002 | stock items, reservations, outbox | Reserves stock all-or-nothing (`StockReserved` / `StockRejected`); releases it on `OrderCancelled` (compensation) |
| `payment-service` | 8003 | payments, outbox | Authorises the card token with a simulated provider after `StockReserved` (`PaymentAuthorised` / `PaymentFailed`) |
| `notification-service` | 8004 | notifications, recipients | Emails the customer about every outcome; nothing waits for it |
| `rabbitmq` | 15672 (UI) | – | Topic exchange `maltashop.events`, quorum queues, dead-letter queue |

```
Client ──POST /orders──▶ Order ──OrderPlaced──▶ Inventory ──StockReserved──▶ Payment
                           ▲                        ▲                          │
                           └──── StockRejected ─────┘    PaymentAuthorised/Failed
                           └─────────────────────────────────────────────────────┘
          Order ──OrderConfirmed / OrderCancelled──▶ Notification (+ Inventory releases stock on OrderCancelled)
```

Shared plumbing (JSON logs, correlation IDs, outbox relay, idempotent consumer, event
bus) lives in a thin chassis library, `libs/chassis`. Business logic never goes there.

## How to run it

Requirements: Docker Desktop (or Docker Engine) with Compose v2. About 1.5 GB of free RAM.

```bash
git clone <this-repo-url> maltashop-prototype
cd maltashop-prototype
docker compose up --build          # first build takes a few minutes
```

When all containers report `healthy`, in a second terminal:

```bash
# place an order (PowerShell users: use curl.exe and escape the quotes, or use the smoke test)
curl -i -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: demo-001" \
  -H "Idempotency-Key: demo-001" \
  -d '{"customer_id":"cust-001","customer_email":"maria.borg@example.com.mt","payment_token":"tok_visa_ok","items":[{"sku":"KETTLE-1L","quantity":2,"unit_price_cents":2499}]}'

curl http://localhost:8001/orders/<order_id>                 # PENDING -> CONFIRMED
curl "http://localhost:8004/notifications?order_id=<order_id>"
curl http://localhost:8002/stock/KETTLE-1L

# follow one request through every service
docker compose logs --no-log-prefix | python scripts/trace.py demo-001

# full end-to-end check (happy path, declined card, out of stock)
python scripts/smoke_test.py
```

Try `"payment_token":"tok_decline"` to see the compensating action release stock, or
SKU `AIRFRYER-XL` (seeded with 0 units) to see the order rejected.
Interactive API docs: http://localhost:8001/docs (and 8002–8004).
RabbitMQ UI: http://localhost:15672 (user `maltashop`, password `maltashop-dev`).

Stop with `docker compose down` (add `-v` to wipe the databases).

### Running the tests without Docker

```bash
cd services/order
pip install -r requirements-dev.txt ../../libs/chassis
pytest tests/unit tests/component tests/contract
```

## Environment variables

All configuration comes from the environment (twelve-factor). Defaults suit local
development; copy `.env.example` to `.env` to override them.

| Variable | Used by | Default | Purpose |
|---|---|---|---|
| `SERVICE_NAME` | all | `<name>-service` | Name written into every log line |
| `DATABASE_URL` | all | set in compose | SQLAlchemy async URL of the service's own database |
| `RABBITMQ_URL` | all | `amqp://maltashop:maltashop-dev@rabbitmq:5672/` | Broker connection |
| `BROKER_ENABLED` | all | `true` | `false` runs a service without the broker (tests) |
| `LOG_LEVEL` | all | `INFO` | Log verbosity |
| `OUTBOX_POLL_INTERVAL` | order | `0.5` | Seconds between outbox relay polls |
| `SEED_STOCK` | inventory | `TV-55-OLED=5,KETTLE-1L=50,AIRFRYER-XL=0,SOFA-3S=2` | Initial stock |
| `PROVIDER_TIMEOUT_SECONDS` | payment | `3` | Timeout on the payment-provider call |
| `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | compose | `maltashop` / `maltashop-dev` | Broker credentials |
| `*_DB_PASSWORD` | compose | `<service>-dev` | Database passwords |
| `IMAGE_TAG` | compose | `local` | Image tag (the pipeline sets it to the commit SHA) |

## Design notes

* **Consistency without a distributed transaction** – each step is a local ACID
  transaction that also writes the next event to an `outbox` table; a relay publishes
  it with publisher confirms. Delivery is at-least-once.
* **Idempotency** – every consumer records the `event_id` in `processed_messages` in
  the same transaction as its change, so redeliveries are ignored. `POST /orders`
  honours an `Idempotency-Key` header; the payment provider call is keyed by order ID.
* **Poison messages** – queues are quorum queues with `x-delivery-limit: 5`; after that,
  or at once for contract violations, messages go to `maltashop.dead-letter`.
* **Contracts** – consumers publish JSON Schemas of what they need from `OrderPlaced`
  in `contracts/`; the Order pipeline verifies against them.
* **Correlation IDs** – taken from `X-Correlation-ID` (or generated), carried in
  message headers and written to every JSON log line.

Architecture decision records are in [`docs/adr`](docs/adr).

## CI/CD

`.github/workflows/order-service.yml`: lint → unit → component → contract tests →
build the image once (tagged with the commit SHA) → Trivy scan → end-to-end saga test
on that same image → publish to GHCR → promote the same digest to staging and
production (production can be gated by required reviewers on the GitHub environment).
Deploy steps are simulated because no cluster is attached to the prototype.

## Known limitations

Polling outbox relay (adds up to 0.5 s latency), no API gateway or authentication,
simulated payment provider and email, schema created at start-up instead of
migrations, and no Kubernetes manifests. See section 6.3 of the report.
