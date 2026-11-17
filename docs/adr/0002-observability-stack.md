# ADR 0002 – OpenTelemetry with a managed Grafana stack for observability

* Status: Accepted
* Deciders: Lead Solutions Architect, operations staff

## Context

Today logs are local files on two VMs, there are no dashboards and no alerts, and most
incidents are reported by customers. With several services, one customer request now
crosses process boundaries, so logs alone are not enough: MaltaShop needs logs, metrics
and traces joined by a correlation/trace ID, plus SLO-based alerting. Two operations
staff cannot run a large self-hosted search cluster.

Criteria: (1) effort to operate, (2) cost that grows predictably with volume,
(3) vendor neutrality of the instrumentation, (4) ability to correlate logs, metrics
and traces, (5) EU data residency for GDPR.

## Decision

Instrument every service with OpenTelemetry SDKs and send telemetry through the
OpenTelemetry Collector to a managed Grafana stack (Loki for logs, Prometheus-compatible
metrics, Tempo for traces) hosted in an EU region. Structured JSON logs follow the
mandatory MaltaShop schema. Alerts are defined on SLOs and routed to the on-call phone.

## Alternatives rejected

* **Self-hosted ELK/OpenSearch** – powerful search, but the cluster itself becomes a
  production system the two ops staff must size, patch and back up.
* **Commercial all-in-one APM (e.g. Datadog, New Relic)** – least effort, but per-host
  and per-GB pricing becomes hard to forecast at sale peaks, and proprietary agents
  raise the cost of leaving.
* **Logs only, no tracing** – cheapest, but cannot show where time is spent across
  a saga that spans four services.

## Consequences

* Positive: one place to look, vendor-neutral instrumentation, low fixed cost.
* Negative / technical debt: telemetry volume must be controlled (sampling, log
  levels, retention) or the bill rises; personal data must be kept out of logs;
  the team must learn PromQL/LogQL.
