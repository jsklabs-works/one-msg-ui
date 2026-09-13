# one-msg-ui

A single UI to ease day-to-day operations and support across heterogeneous messaging systems — **IBM MQ**, **Apache Kafka**, **Solace PubSub+**, **RabbitMQ**, and **ActiveMQ Artemis**, with more on the way. Read-only: monitoring (queue depth, consumer lag, health) plus non-destructive message browsing — not an admin console, and deliberately not an alerting tool (see Scope below).

Each system is fundamentally different (point-to-point queues vs. distributed log vs. topic/queue hybrid), so rather than faking a one-size-fits-all abstraction, this project normalizes each broker's native metrics into a shared schema and builds the UI against that — see [`docs/architecture.md`](docs/architecture.md) for the full design.

## Scope

- **In v1:** cross-broker monitoring (depth/lag/health) + non-destructive message browsing (peek at what's on a queue/topic), across prod/uat/dev from day one.
- **Out of scope, permanently:** alerting/paging — this sits alongside each broker's native alerting tools, never replaces or owns alert rules.
- **Deferred:** admin actions (create/delete/purge/ACLs), produce/consume/replay of messages.
- Full rationale and the resolved open questions (credential storage, environment scope, dashboard vs. drill-down usage) are in [`docs/architecture.md §7`](docs/architecture.md#7-decisions-resolved-2026-09-12).

## Status

- **Kafka adapter** — Phase 1 complete: topic/consumer-group discovery, lag calculation, connectivity + replication (ISR/under-replicated/offline) health, and non-destructive message peek, tested against a local Docker Kafka. See [`adapters/kafka/README.md`](adapters/kafka/README.md) to run it.
- **Solace adapter** — Phase 2 complete: VPN/queue monitoring (SEMP v2), connectivity + spool-usage health, and non-destructive message browsing (via the official `solace-pubsubplus` client), verified against a real broker. See [`adapters/solace/README.md`](adapters/solace/README.md) to run it.
- **IBM MQ adapter** — Phase 3 complete: queue monitoring (MQSC-over-REST), connectivity + capacity health, and non-destructive message peek (REST Messaging API), verified against a real queue manager. No PCF, no native client. See [`adapters/mq/README.md`](adapters/mq/README.md) to run it.
- **RabbitMQ adapter** — Phase 5 complete: vhost/queue monitoring, node connectivity + capacity health, and non-destructive message peek — all through one API (the management HTTP API), no AMQP client needed at all. Verified against a real broker (Docker). See [`adapters/rabbitmq/README.md`](adapters/rabbitmq/README.md) to run it.
- **ActiveMQ Artemis adapter** — Phase 5 complete: address/queue monitoring, broker connectivity + capacity health, and non-destructive message peek — all through Jolokia (JMX-over-HTTP, bundled with the broker's own web console), no JMS/core client needed. Verified against a real broker (Docker). See [`adapters/activemq/README.md`](adapters/activemq/README.md) to run it.
- **API layer** — MVP working: a FastAPI service fanning out to all five adapters, serving the unified model as JSON, with graceful per-broker degradation. Live passthrough, not yet the poller+store pipeline from architecture.md §4 — see [`api/README.md`](api/README.md).
- **UI** — MVP working: React + Vite + TypeScript, tabbed (a Dashboard tab plus one real-route tab per connected broker, so multiple brokers can be inspected simultaneously in separate browser tabs), with per-resource drill-down and message peek. Built and verified end-to-end in a real browser against real data from all five systems. See [`ui/README.md`](ui/README.md).

## Layout

```
adapters/     one module per broker type, each translating that broker's native
              monitoring API into the shared data model (see docs/architecture.md §3-4)
  kafka/
  mq/
  solace/
  rabbitmq/
  activemq/
api/          unified API layer over the normalized model, consumed by the UI
ui/           the dashboard itself
docs/         architecture and design docs
```

## Build order

Per the architecture doc's phased rollout: **Kafka first** (Phase 1), then Solace (Phase 2), then IBM MQ (Phase 3), then RabbitMQ and ActiveMQ Artemis (Phase 5 — Phase 4 was the deferred admin/produce/replay scope, still not built). Kafka is the hardest case to get the abstraction right for (no native "queue depth" concept), so validating the shared schema against it first de-risks the rest. Azure Service Bus and SQS/SNS are next, once there's an emulator/cloud-namespace verification story to match the Docker-broker pattern every adapter so far has used.

## Getting started

**Kafka** (self-contained — spins up its own local broker):

```bash
docker compose up -d          # local single-node Kafka (KRaft, no ZooKeeper)
cd adapters/kafka
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m kafka_adapter.main topics
```

**Solace** (needs a broker you already have, or a local Solace PubSub+ Docker container — not bundled in this repo's docker-compose):

```bash
cd adapters/solace
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m solace_adapter.main queues
```

**IBM MQ** (self-contained — spins up its own local queue manager; amd64-only image, emulated on Apple Silicon so startup is slower):

```bash
docker compose up -d mq       # local queue manager (QM1)
cd adapters/mq
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m mq_adapter.main queues --pattern "DEV.*"
```

**RabbitMQ** (self-contained — spins up its own local broker):

```bash
docker compose up -d rabbitmq   # local broker, management API on :15672
cd adapters/rabbitmq
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m rabbitmq_adapter.main queues
```

**ActiveMQ Artemis** (self-contained — spins up its own local broker):

```bash
docker compose up -d activemq   # local broker, web console + Jolokia on :8161
cd adapters/activemq
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m activemq_adapter.main queues
```

See [`adapters/kafka/README.md`](adapters/kafka/README.md), [`adapters/solace/README.md`](adapters/solace/README.md), [`adapters/mq/README.md`](adapters/mq/README.md), [`adapters/rabbitmq/README.md`](adapters/rabbitmq/README.md), and [`adapters/activemq/README.md`](adapters/activemq/README.md) for the full picture — message peek/browsing, connection flags, and running each test suite.

**Full stack, once the brokers above are up:**

```bash
cd api
python3 -m venv .venv
./.venv/bin/pip install -e ../adapters/kafka -e ../adapters/solace -e ../adapters/mq -e ../adapters/rabbitmq -e ../adapters/activemq -e ".[dev]"
./.venv/bin/uvicorn api.main:app --port 8010          # in one terminal

cd ui
npm install
npm run dev                                            # in another; opens on :5173
```

See [`api/README.md`](api/README.md) and [`ui/README.md`](ui/README.md) for endpoint details, broker configuration, and what's verified vs. not.
