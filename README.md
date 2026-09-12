# one-msg-ui

A single UI to ease day-to-day operations and support across heterogeneous messaging systems — starting with **IBM MQ**, **Apache Kafka**, and **Solace PubSub+**. Read-only: monitoring (queue depth, consumer lag, health) plus non-destructive message browsing — not an admin console, and deliberately not an alerting tool (see Scope below).

Each system is fundamentally different (point-to-point queues vs. distributed log vs. topic/queue hybrid), so rather than faking a one-size-fits-all abstraction, this project normalizes each broker's native metrics into a shared schema and builds the UI against that — see [`docs/architecture.md`](docs/architecture.md) for the full design.

## Scope

- **In v1:** cross-broker monitoring (depth/lag/health) + non-destructive message browsing (peek at what's on a queue/topic), across prod/uat/dev from day one.
- **Out of scope, permanently:** alerting/paging — this sits alongside each broker's native alerting tools, never replaces or owns alert rules.
- **Deferred (Phase 4):** admin actions (create/delete/purge/ACLs), produce/consume/replay of messages, additional broker types.
- Full rationale and the resolved open questions (credential storage, environment scope, dashboard vs. drill-down usage) are in [`docs/architecture.md §7`](docs/architecture.md#7-decisions-resolved-2026-09-12).

## Status

- **Kafka adapter** — Phase 1 complete: topic/consumer-group discovery, lag calculation, connectivity + replication (ISR/under-replicated/offline) health, and non-destructive message peek, tested against a local Docker Kafka. See [`adapters/kafka/README.md`](adapters/kafka/README.md) to run it.
- **Solace, IBM MQ adapters, API layer, UI** — not started (Phases 2-3 and beyond).

## Layout

```
adapters/     one module per broker type, each translating that broker's native
              monitoring API into the shared data model (see docs/architecture.md §3-4)
  kafka/
  mq/
  solace/
api/          unified API layer over the normalized model, consumed by the UI
ui/           the dashboard itself
docs/         architecture and design docs
```

## Build order

Per the architecture doc's phased rollout: **Kafka first** (Phase 1), then Solace (Phase 2), then IBM MQ (Phase 3). Kafka is the hardest case to get the abstraction right for (no native "queue depth" concept), so validating the shared schema against it first de-risks the other two adapters.

## Getting started

```bash
docker compose up -d          # local single-node Kafka (KRaft, no ZooKeeper)
cd adapters/kafka
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m kafka_adapter.main topics
```

See [`adapters/kafka/README.md`](adapters/kafka/README.md) for the full picture, including message peek and running the test suite.
