# Kafka adapter

**Status:** MVP working (Phase 1) — topic/consumer-group discovery, lag calculation, health connectivity check, and non-destructive message peek are implemented and tested against a local cluster. JMX-based broker internals (ISR/under-replicated-partition detail) are not wired up yet.

## Responsibility

Translate Kafka's native monitoring surface into the shared model defined in [`/docs/architecture.md`](../../docs/architecture.md#3-unified-data-model):

- `Resource` — one per topic, with `consumer_lag` populated (Kafka has no native "depth" concept; `depth_current`/`depth_max` stay null)
- `ConsumerGroup` — group state, member count, and per-partition `{log_end_offset, committed_offset, lag}`
- `HealthEvent` — broker/ISR/under-replicated-partition issues
- `MessageSample` — non-destructive peek at a topic/partition's messages (see [`/docs/architecture.md §3.1`](../../docs/architecture.md#31-message-browsingpeek-in-scope-for-v1)): a scratch/no-commit consumer group or `assign()` + manual `seek()`, polling without ever committing offsets

## Data sources

- **Kafka Admin API** — offsets and consumer group metadata (primary source), via [`kafka-python-ng`](https://pypi.org/project/kafka-python-ng/) (a maintained fork of `kafka-python`; same `kafka` import namespace, but with Python 3.12+ compatibility fixes)
- **JMX** — broker internals, `records-lag-max` client-side metric, replication/ISR status. **Not implemented yet** — the current `get_health_events()` only reports basic cluster-metadata connectivity, not ISR/under-replication. Add JMX once that detail is actually needed.

## Known gotchas to handle

- Consumers using `assign()` instead of `subscribe()` don't report lag through the standard group-metadata path — this adapter always computes lag directly (`log-end-offset - committed-offset`) via the Admin API rather than trusting a single source (see [`lag.py`](src/kafka_adapter/lag.py)).
- Consumer groups that sit `EMPTY` for more than a day stop emitting lag metrics — the adapter distinguishes "no lag data" (`None`) from "zero lag" (`0`) throughout; see `compute_lag`/`sum_topic_lag` in [`lag.py`](src/kafka_adapter/lag.py) and their tests in [`tests/test_lag.py`](tests/test_lag.py).

## Layout

```
src/kafka_adapter/
  models.py   normalized dataclasses (Resource, ConsumerGroup, HealthEvent, MessageSample)
  client.py   KafkaAdapter — topic/consumer-group discovery, lag calc, health
  lag.py      pure lag-calculation helpers (unit-tested, no broker needed)
  peek.py     non-destructive message browsing
  main.py     CLI entry point (`kafka-adapter topics` / `kafka-adapter peek <topic>`)
scripts/seed.py   dev-only: creates a test topic + lagging consumer group against docker-compose
tests/            pytest suite for lag.py and peek.py's pure logic
```

## Running it locally

```bash
# from repo root — starts a single-node KRaft Kafka on localhost:9092
docker compose up -d

cd adapters/kafka
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"

# optional: seed a topic with messages and a lagging consumer group
./.venv/bin/python scripts/seed.py

./.venv/bin/python -m kafka_adapter.main topics
./.venv/bin/python -m kafka_adapter.main peek orders --limit 5

./.venv/bin/pytest
```

## Next step

Decide how this CLI becomes a real service: wire it into the ingestion/poller described in [`/docs/architecture.md §4`](../../docs/architecture.md#4-architecture) (scheduled polling, writing into the metrics/metadata store) rather than being invoked ad hoc. Add JMX-based health detail once broker/ISR monitoring is actually needed.
