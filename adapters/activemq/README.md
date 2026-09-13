# ActiveMQ Artemis adapter

**Status:** Phase 5 complete — address/queue monitoring, broker connectivity + capacity health, and non-destructive message browsing are all implemented and verified against a real Artemis broker (Docker, `apache/activemq-artemis:latest`).

## Responsibility

Translate Artemis's JMX management surface (via Jolokia) into the shared model defined in [`/docs/architecture.md`](../../docs/architecture.md#3-unified-data-model):

- `Resource` — one per queue (`MessageCount` → `depth_current`; the queue's `RingSize` — a hard per-queue message-count cap — when one is actually set, → `depth_max`). Most Artemis queues don't set a `RingSize` at all — flow control here is normally done at the *address* level via a shared memory limit, not a per-queue message count — so this is None far more often than not, same posture as RabbitMQ's mostly-unbounded queues.
- Artemis is scoped two levels deep (**address → queue**, a queue is always bound to exactly one address) — kept explicit as `Resource.namespace`, same shape as Solace's VPN/MQ's queue manager/RabbitMQ's vhost. Unlike those three, there's no vhost/VPN-style multi-tenant filter to offer: one broker connection just discovers every address/queue its credentials can see, no optional scoping parameter needed.
- `HealthEvent` — one **connectivity** event (`Started`/`Active` on the broker MBean — `Active` false but `Started` true means a passive/backup node in a replicated pair, a warning rather than critical), one **capacity** event for broker-wide memory pressure (`AddressMemoryUsage` vs. `GlobalMaxSize` — Artemis throttles/blocks producers as this climbs, the closest analogue to RabbitMQ's `mem_alarm`), and one **capacity** event summarizing bounded queues against their `RingSize` (mirrors RabbitMQ's `x-max-length` handling: says "no ring-size limits configured" rather than a misleading "0/0 over threshold" when nothing's bounded).
- `MessageSample` — non-destructive peek at a queue's messages, including `topic`: the address the message arrived on, Artemis's closest analogue to Kafka's topic / Solace's topic-subscription match / RabbitMQ's routing key.

## Data source: one API for everything

Like RabbitMQ, this adapter needs exactly one thing: **Jolokia** (JMX-over-HTTP), bundled with Artemis's own web console and enabled by default (see the broker's own startup log: `"Artemis Jolokia REST API available at http://.../console/jolokia"`). It covers monitoring *and* peek:

- `read`/`search` on the broker MBean and each queue's MBean (`org.apache.activemq.artemis:...,component=addresses,...,subcomponent=queues`) — plain JSON, used for health and resources.
- `exec` the queue MBean's own `browse()` operation — Artemis's real, broker-native non-destructive queue browse (the same mechanism its JMS `QueueBrowser` API uses under the hood), not a consume-and-hope workaround. No JMS/core client (or JMX-over-RMI) used anywhere in this adapter.

**Verified live, non-destructively:** created a queue via the Artemis CLI, published 4 messages, browsed them via Jolokia, confirmed `MessageCount` was unchanged (4 before and after), browsed again and got the same 4 messages back — repeatable, like Kafka's/Solace's/RabbitMQ's peek, unlike MQ's one-shot limitation.

**Two real things found live, not assumed:**
- Jolokia's `list` operation (metadata discovery — attribute/operation names) needs its own path-escaping for ObjectName values containing quotes (every Artemis MBean name does: `broker="0.0.0.0"`), and got badly confused more than once during exploration. `search`/`read`/`exec` with the mbean passed as a plain JSON string field don't have this problem — this adapter uses those exclusively, never `list`.
- A JMX ObjectName search pattern needs a **trailing bare `*`** to match mbeans with properties the pattern doesn't mention (e.g. `broker=`) — a wildcarded *value* on a named property (`routing-type=*`) does the opposite of what it looks like: it still requires the mbean's property *set* to match exactly, and matches nothing once an unlisted property (`broker=`) is present. Confirmed by testing both forms directly against a real broker (see `client.py`'s and `peek.py`'s comments for the exact patterns).

## Layout

```
src/activemq_adapter/
  models.py   normalized dataclasses (Resource, HealthEvent, MessageSample)
  client.py   ActiveMQAdapter — Jolokia address/queue discovery, broker + capacity health
  health.py   pure health-classification helpers (unit-tested, no broker needed)
  peek.py     non-destructive message browsing via the queue MBean's browse() operation
  main.py     CLI entry point (`activemq-adapter queues` / `activemq-adapter peek <queue>`)
scripts/seed.py   dev-only: creates a test queue + publishes messages via the Artemis CLI
                  (docker exec, not REST — see the script's own docstring for why)
tests/            pytest suite for health.py, peek.py, and client.py's mockable HTTP logic
```

## Running it locally

This repo's [`docker-compose.yml`](../../docker-compose.yml) runs an `apache/activemq-artemis:latest` container — `admin`/`admin` via `ARTEMIS_USER`/`ARTEMIS_PASSWORD`.

```bash
docker compose up -d activemq

cd adapters/activemq
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"

# optional: seed a queue with test messages (needs the container name
# from docker-compose.yml, "one-msg-ui-activemq" — see scripts/seed.py)
./.venv/bin/python scripts/seed.py

./.venv/bin/python -m activemq_adapter.main queues
./.venv/bin/python -m activemq_adapter.main peek orders --limit 5

./.venv/bin/pytest
```

Flags: `--console-url` (default `http://localhost:8161`, the web console's own base — Jolokia's path is appended internally), `--username`, `--password`. `peek` also takes `--address` (the address the queue is bound to; defaults to the queue name itself, the common case for an auto-created address matching its queue).

**Real limitation, not worked around:** `browse()` has no server-side limit or pagination — it always returns the whole queue's contents, and `limit` is applied client-side after the fact. Fine for an inspection-sized queue; a queue with thousands of messages means a proportionally large response before this ever gets to trim it down. Separately, `browse()`'s JSON only carries a `text` field for TEXT-type messages (what every adapter's own seed script publishes) — a BytesMessage/ObjectMessage/StreamMessage has no body field in this API surface at all, surfaced honestly as `"[non-text message, N bytes]"` rather than guessing at a decode.

## Auth

Basic auth against Jolokia — the same credentials cover monitoring and peek, same as RabbitMQ (and unlike MQ's admin/app credential split). TLS is supported (`https://` console URL) but not exercised here since the dev broker runs plain HTTP; pass `verify_certificate=False` to `ActiveMQAdapter` for a self-signed cert, same convention as the other adapters.

## Next step

Wire into the ingestion/poller described in [`/docs/architecture.md §4`](../../docs/architecture.md#4-architecture), same as the other four. Azure Service Bus and SQS/SNS remain further out — no local Docker broker for either, so verification would need a cloud namespace or an emulator rather than the docker-compose pattern every adapter so far has used.
