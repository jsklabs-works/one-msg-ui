# RabbitMQ adapter

**Status:** Phase 5 complete — vhost/queue monitoring, node connectivity + capacity health, and non-destructive message browsing are all implemented and verified against a real RabbitMQ broker (Docker, `rabbitmq:4-management`).

## Responsibility

Translate RabbitMQ's management HTTP API into the shared model defined in [`/docs/architecture.md`](../../docs/architecture.md#3-unified-data-model):

- `Resource` — one per queue (`messages` → `depth_current`; the queue's `x-max-length` policy, when one is set, → `depth_max`). Exchanges route messages but don't hold them, so they're not resources here — same reasoning as Kafka not surfacing consumer groups as resources.
- RabbitMQ is scoped two levels deep (**vhost → queue**), same shape as Solace's VPN and MQ's queue manager — kept explicit as `Resource.namespace`, never flattened. **A single adapter/broker connection spans every vhost its credentials can see, discovered dynamically** — `vhost` is an optional filter to pin to one, not a required scope (the same pattern as Solace's `vpn_name`, adopted from the start this time instead of being a bug fixed later).
- `HealthEvent` — one **connectivity** event per cluster node (`running`/`mem_alarm`/`disk_free_alarm`), plus one **capacity** event summarizing queues against their `x-max-length` policy. Most RabbitMQ queues have no length limit at all — unlike MQ, where MAXDEPTH always exists — so the capacity event says as much rather than reporting "0/0 over threshold" as if every queue had been checked and passed.
- `MessageSample` — non-destructive peek at a queue's messages, including `topic`: the message's routing key, RabbitMQ's closest analogue to Kafka's topic / Solace's topic-subscription match (the actual thing that routed the message onto this queue, not necessarily the queue's own name).

## Data source: one API for everything

Unlike the other three adapters, this one needs exactly one thing: the **management HTTP API** (the `rabbitmq_management` plugin, enabled by default in every `rabbitmq:*-management` image). It covers monitoring *and* peek:

- `GET /api/nodes`, `GET /api/vhosts`, `GET /api/queues/{vhost}` — plain JSON, used for health and resources.
- `POST /api/queues/{vhost}/{queue}/get` with `ackmode: "ack_requeue_true"` — delivers messages and immediately requeues them server-side. This is RabbitMQ's own documented mechanism for "get messages without removing them," not a consume-and-hope workaround — same non-destructive guarantee as Solace's queue browser or MQ's REST peek, but needing no second connection type at all. No AMQP client (`pika` or similar) is used anywhere in this adapter.

**Verified live, non-destructively:** published 4 messages to a real queue, peeked them, confirmed queue depth was unchanged (`messages: 4` before and after), peeked again and got the same 4 messages back — not a one-shot browse cursor, repeatable like Kafka's and Solace's peek.

**One real quirk found live:** the management API's queue/message counters are *sampled*, not instantaneous — a `GET` immediately after publishing can still show the pre-publish count for a couple of seconds. `scripts/seed.py` accounts for this with a short sleep before its own verification read; the adapter itself doesn't need to (a UI poll a few seconds later sees the real number regardless).

## Layout

```
src/rabbitmq_adapter/
  models.py   normalized dataclasses (Resource, HealthEvent, MessageSample)
  client.py   RabbitMQAdapter — management API vhost/queue discovery, node + capacity health
  health.py   pure health-classification helpers (unit-tested, no broker needed)
  peek.py     non-destructive message browsing via POST .../get with ackmode=ack_requeue_true
  main.py     CLI entry point (`rabbitmq-adapter queues` / `rabbitmq-adapter peek <queue>`)
scripts/seed.py   dev-only: creates a bounded test queue + publishes messages via the management API
tests/            pytest suite for health.py, peek.py, and client.py's mockable HTTP logic
```

## Running it locally

This repo's [`docker-compose.yml`](../../docker-compose.yml) runs a `rabbitmq:4-management` container — `admin`/`admin` (explicit env vars, not the default `guest`/`guest`, which RabbitMQ restricts to loopback-only connections and is easy to trip over from inside Docker's networking).

```bash
docker compose up -d rabbitmq

cd adapters/rabbitmq
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"

# optional: seed a queue with test messages
./.venv/bin/python scripts/seed.py

./.venv/bin/python -m rabbitmq_adapter.main queues                  # discovers every vhost
./.venv/bin/python -m rabbitmq_adapter.main --vhost / queues         # or just one
./.venv/bin/python -m rabbitmq_adapter.main --vhost / peek orders --limit 5

./.venv/bin/pytest
```

Flags: `--api-url` (default `http://localhost:15672`), `--vhost` (optional for `queues` — omit to discover all; **required** for `peek`, so it's unambiguous which vhost's queue to hit when two vhosts have a same-named queue), `--username`, `--password`.

## Auth

Basic auth against the management API — the same credentials cover monitoring and peek, unlike Solace's split between SEMP admin credentials and per-VPN SMF authentication. TLS is supported by the management API (`https://` base URL) but not exercised here since the dev broker runs plain HTTP; pass `verify_certificate=False` to `RabbitMQAdapter` for a self-signed cert, same convention as the other adapters.

## Next step

Wire into the ingestion/poller described in [`/docs/architecture.md §4`](../../docs/architecture.md#4-architecture), same as the other three. ActiveMQ/Artemis is next per the build order.
