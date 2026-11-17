# Consumer-driven message contracts

Each consumer of an event owns a JSON Schema describing **only the fields it
relies on**. The producer's pipeline verifies every event it emits against
every consumer's schema, so a breaking change fails the producer's build
before it reaches a shared environment.

| Event | Producer | Consumer contracts |
|---|---|---|
| `OrderPlaced` v1 | order-service | `order-placed/inventory-service.schema.json`, `order-placed/payment-service.schema.json`, `order-placed/notification-service.schema.json` |

Rules: consumers ignore unknown fields (tolerant reader); producers may add
optional fields but must not remove or rename a field any consumer requires.
A breaking change needs a new event version (`OrderPlaced` v2) published
alongside v1 until all consumers have moved.
