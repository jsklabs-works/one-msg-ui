# Unified API layer

**Status:** MVP working — a FastAPI service that fans out to all three broker adapters and serves the normalized model (`Broker`, `Resource`, `ConsumerGroup`, `HealthEvent`, `MessageSample`) as JSON, verified end-to-end against the same three real local brokers the adapters were each verified against.

## Responsibility

Serves the normalized model (see [`/docs/architecture.md`](../docs/architecture.md#3-unified-data-model)) to the UI. This is the layer where "kafka has consumer_lag, MQ has depth, Solace has spool usage" actually gets flattened into one shape the UI can render without caring which broker a resource came from — see [`normalize.py`](src/api/normalize.py), which converts each adapter's own dataclasses into the shared pydantic models in [`models.py`](src/api/models.py).

**Important scope note:** this is a **live passthrough**, not the ingestion/poller + time-series store pipeline described in [architecture.md §4](../docs/architecture.md#4-architecture). Every request to `/api/resources` or `/api/health` queries all three brokers live, synchronously. That's fine for an MVP with three local dev brokers; it is *not* how this should work once there are real brokers and a UI polling every few seconds — that needs the poller-writes-to-a-store design the architecture doc describes, so brokers get polled once on a schedule instead of once per UI request. Treat this API as a proof that the normalization/fan-out logic works, not as the production data path.

## Endpoints

| Method & path | Returns |
|---|---|
| `GET /api/brokers` | All configured brokers, with live `status` (up/down) |
| `GET /api/resources` | All resources across all brokers. Filter with `?environment=`, `?system_type=`, `?broker_id=` |
| `GET /api/resources/{resource_id}` | One resource (404 if not found) |
| `GET /api/health` | All health events. Filter with `?broker_id=` |
| `GET /api/consumer-groups` | Kafka consumer groups (empty for non-Kafka brokers). Filter with `?broker_id=` |
| `GET /api/resources/{resource_id}/messages?limit=N` | Non-destructive peek — architecture.md §3.1. Kafka/Solace can return up to `limit`; MQ can only ever return the one oldest message (see [`adapters/mq/README.md`](../adapters/mq/README.md)) |

A broker that's unreachable is reported with `status: "down"` and a `critical`/`connectivity` `HealthEvent` rather than failing the whole request — see [`aggregator.py`](src/api/aggregator.py)'s `fetch_all()`. Two-out-of-three brokers responding is more useful to an on-call engineer than a blank screen.

## Broker configuration

[`config/brokers.json`](config/brokers.json) lists the brokers this instance talks to and their connection details, loaded by [`config.py`](src/api/config.py). Per [architecture.md §7](../docs/architecture.md#7-decisions-resolved-2026-09-12), credentials belong in an encrypted Postgres store long-term — this plain JSON file is an explicit MVP placeholder, not a decision to keep committing plaintext credentials. The checked-in file only has the well-known local dev defaults already documented in each adapter's README (not real secrets); replace it (or point `ONE_MSG_UI_BROKERS_CONFIG` at a different path) before this ever talks to anything real.

## Layout

```
src/api/
  models.py      the unified pydantic models (the actual shared schema, made real)
  config.py      loads config/brokers.json into BrokerConfig objects
  normalize.py   pure per-adapter-type -> unified-model conversion functions
  aggregator.py  BrokerRegistry — fans out to the three adapters, handles broker-down gracefully
  main.py        FastAPI app and routes
config/brokers.json   broker inventory for local dev (see note above)
tests/                pytest suite: normalize.py against real adapter dataclasses, aggregator.py's error handling
```

## Running it locally

Needs all three adapters' dev brokers up first (Kafka via this repo's `docker-compose.yml`, Solace and MQ per their own READMEs).

```bash
cd api
python3 -m venv .venv
./.venv/bin/pip install -e ../adapters/kafka -e ../adapters/solace -e ../adapters/mq -e ".[dev]"

./.venv/bin/uvicorn api.main:app --reload --port 8010
# NOTE: something on this machine's Docker networking already claims :8000
# (an IPv6 listener with nothing behind it) — use a different port, don't
# fight it, see git history if you need the story.

curl http://localhost:8010/api/brokers
curl http://localhost:8010/api/resources
curl "http://localhost:8010/api/resources/local-kafka:orders/messages?limit=5"

./.venv/bin/pytest
```

## Next step

Build the UI against this (React + Vite + TypeScript — see [`/ui/README.md`](../ui/README.md)). Longer-term, replace the live-passthrough aggregator with the real poller + time-series store pipeline once this needs to handle more than a handful of local brokers.
