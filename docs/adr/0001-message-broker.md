# ADR 0001 – Use RabbitMQ as the message broker

* Status: Accepted
* Deciders: Lead Solutions Architect, operations staff, data team lead

## Context

MaltaShop's services must exchange domain events (OrderPlaced, StockReserved,
PaymentAuthorised ...) and the AI recommendation model needs browsing and order events
in near real time without touching checkout. Two operations staff run the platform,
nobody has run a distributed log in production, and the tooling budget is small.
Event volume is modest: a Malta-and-Gozo storefront, peaking at ten times normal
traffic during sales.

Criteria: (1) operational effort for two people, (2) cost, (3) delivery guarantees
(durable, acknowledged, dead-lettering), (4) support for replay by the data team,
(5) skills available in the local market, (6) avoid central "smart pipes".

## Decision

Use RabbitMQ (4.x) on a managed EU-hosted plan, with one durable topic exchange,
quorum queues per consumer, publisher confirms, dead-letter queues and the
transactional outbox in each producer. Browsing events go to a RabbitMQ stream that
the Recommendation service can replay.

## Alternatives rejected

* **Apache Kafka (self-managed or managed)** – best for very high throughput and long
  replay, but more moving parts and a higher monthly floor than MaltaShop's volume
  justifies; the team has no experience operating it.
* **Vendor ESB** – routing and transformation logic would sit inside the bus,
  recreating the coupling the migration is meant to remove, with licence lock-in.
* **Cloud-native queue only (e.g. SQS/SNS)** – cheap and simple, but ties the event
  model to one provider and offers no replay for the data team.

## Consequences

* Positive: familiar AMQP model, low running cost, the prototype already uses it.
* Negative / technical debt: long-term event history is not the broker's job, so the
  Recommendation service must store the events it needs. If event volume or replay
  needs grow (e.g. several ML models), migrating to Kafka is likely; the outbox and
  event envelope keep that change contained.
