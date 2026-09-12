# Kafka adapter

**Status:** Phase 1 complete — topic/consumer-group discovery, lag calculation, connectivity + replication health, and non-destructive message peek are all implemented and tested against a local cluster. Deeper broker-resource metrics (request latency percentiles, handler idle ratio) would need JMX and aren't covered — see Data sources below.

## Responsibility

Translate Kafka's native monitoring surface into the shared model defined in [`/docs/architecture.md`](../../docs/architecture.md#3-unified-data-model):

- `Resource` — one per topic, with `consumer_lag` populated (Kafka has no native "depth" concept; `depth_current`/`depth_max` stay null)
- `ConsumerGroup` — group state, member count, and per-partition `{log_end_offset, committed_offset, lag}`
- `HealthEvent` — connectivity (cluster metadata reachable) and replication (ISR/under-replicated/offline-replica status, per partition — see `replication.py`)
- `MessageSample` — non-destructive peek at a topic/partition's messages (see [`/docs/architecture.md §3.1`](../../docs/architecture.md#31-message-browsingpeek-in-scope-for-v1)): a scratch/no-commit consumer group or `assign()` + manual `seek()`, polling without ever committing offsets

## Data sources

- **Kafka Admin API** — offsets, consumer group metadata, and per-partition `replicas`/`isr`/`offline_replicas` from topic metadata (primary source for everything this adapter does), via [`kafka-python-ng`](https://pypi.org/project/kafka-python-ng/) (a maintained fork of `kafka-python`; same `kafka` import namespace, but with Python 3.12+ compatibility fixes)
- **JMX** — turned out *not* to be needed for ISR/under-replicated-partition status, contrary to the original architecture doc's assumption: `describe_topics()`'s partition metadata already carries `replicas`/`isr`/`offline_replicas` straight from the same Metadata API a broker-side JMX metric would derive from (see [`replication.py`](src/kafka_adapter/replication.py)). JMX would still matter for broker-resource metrics the Metadata API doesn't expose — request latency percentiles, handler idle ratio — but nothing in this adapter's current scope needs those. Not implemented.

## Known gotchas to handle

- Consumers using `assign()` instead of `subscribe()` don't report lag through the standard group-metadata path — this adapter always computes lag directly (`log-end-offset - committed-offset`) via the Admin API rather than trusting a single source (see [`lag.py`](src/kafka_adapter/lag.py)).
- Consumer groups that sit `EMPTY` for more than a day stop emitting lag metrics — the adapter distinguishes "no lag data" (`None`) from "zero lag" (`0`) throughout; see `compute_lag`/`sum_topic_lag` in [`lag.py`](src/kafka_adapter/lag.py) and their tests in [`tests/test_lag.py`](tests/test_lag.py).

## Layout

```
src/kafka_adapter/
  models.py        normalized dataclasses (Resource, ConsumerGroup, HealthEvent, MessageSample)
  client.py        KafkaAdapter — topic/consumer-group discovery, lag calc, health
  lag.py           pure lag-calculation helpers (unit-tested, no broker needed)
  replication.py   pure ISR/replication classification (unit-tested, no broker needed)
  peek.py          non-destructive message browsing
  main.py          CLI entry point (`kafka-adapter topics` / `kafka-adapter peek <topic>`)
scripts/seed.py   dev-only: creates a test topic + lagging consumer group against docker-compose
tests/            pytest suite for lag.py, replication.py, and peek.py's pure logic
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

Decide how this CLI becomes a real service: wire it into the ingestion/poller described in [`/docs/architecture.md §4`](../../docs/architecture.md#4-architecture) (scheduled polling, writing into the metrics/metadata store) rather than being invoked ad hoc.

Note: the docker-compose cluster is single-broker/replication-factor-1, so under-replicated/offline partitions can't be triggered against it — `replication.py`'s classification logic is verified with synthetic data in `tests/test_replication.py` instead. Confirm against a real multi-broker cluster before relying on it in production.
