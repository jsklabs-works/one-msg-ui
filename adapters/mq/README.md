# IBM MQ adapter

**Status:** Phase 3 complete — queue monitoring (MQSC-over-REST), connectivity + capacity health, and non-destructive message peek (REST Messaging API) are all implemented and verified against a real IBM MQ queue manager. No PCF, no native client (pymqi) — everything is plain HTTPS/JSON, same dependency profile as the Kafka and Solace adapters.

## Responsibility

Translate IBM MQ's native monitoring surface into the shared model defined in [`/docs/architecture.md`](../../docs/architecture.md#3-unified-data-model):

- `Resource` — one per local queue, with `depth_current` (`CURDEPTH`) and `depth_max` (`MAXDEPTH`) populated; `consumer_lag` stays null (no per-consumer offset concept in MQ)
- `HealthEvent` — queue manager status (connectivity) and per-queue capacity (depth vs. `MAXDEPTH`)
- `MessageSample` — non-destructive peek at a queue's oldest message (see [`/docs/architecture.md §3.1`](../../docs/architecture.md#31-message-browsingpeek-in-scope-for-v1))

## Data sources — and what we actually found

The architecture doc assumed "REST Admin API, PCF fallback only for what REST doesn't cover." Verified against a real queue manager that the real picture is a bit different:

- **The REST Admin API's plain resource endpoints are config-only.** `GET .../admin/qmgr/{qmgr}/queue/{name}?attributes=curdepth` is rejected outright — `curdepth`/`maxdepth`/`ipprocs` aren't valid attributes for that resource at all, at any casing.
- **Runtime status (depth, open-handle counts, qmgr status) comes through MQSC-over-REST instead** — `POST .../admin/action/qmgr/{qmgr}/mqsc` with `{"type":"runCommand","parameters":{"command":"DISPLAY QUEUE(...) CURDEPTH MAXDEPTH"}}`, which runs a real MQSC command and returns its console text output (`QUEUE(name) CURDEPTH(0) MAXDEPTH(5000)` style) rather than structured JSON. See [`mqsc.py`](src/mq_adapter/mqsc.py) for the parser this requires.
- **No PCF and no native client (pymqi) needed at all.** MQSC-over-REST covers everything this adapter needs (queue depth, open counts, queue manager status). PCF would only matter for attributes MQSC itself can't surface — none came up.
- **Message peek needed real testing to pin down, not assumption.** `GET .../messaging/qmgr/{qmgr}/queue/{q}/message` **does not remove the message it returns** — verified by publishing 2 distinct messages and calling GET repeatedly: it kept returning the same (oldest) message and queue depth never changed. No special "browse" query parameter exists or is needed for this. The real limitation: this endpoint has **no browse cursor** — it always returns the same head-of-queue message, so unlike Kafka/Solace's `peek_messages(limit=N)`, this adapter can only ever surface one message per queue (the oldest), not the next N. A real multi-message non-destructive browse needs `MQGMO_BROWSE_NEXT` via MQI/pymqi, deliberately not used here (see the peek-approach decision below).

## Layout

```
src/mq_adapter/
  models.py   normalized dataclasses (Resource, HealthEvent, MessageSample)
  client.py   MQAdapter — MQSC-over-REST queue/qmgr discovery, health
  mqsc.py     pure MQSC-output parser (unit-tested, no queue manager needed)
  health.py   pure health-classification helpers (unit-tested, no queue manager needed)
  peek.py     non-destructive message peek via the REST Messaging API's plain GET
  main.py     CLI entry point (`mq-adapter queues` / `mq-adapter peek <queue>`)
scripts/seed.py   dev-only: publishes test messages via REST Messaging API PUT
tests/            pytest suite for mqsc.py, health.py, and client.py's mockable logic
```

## Running it locally

```bash
docker compose up -d mq       # local queue manager (see docker-compose.yml — amd64 image, emulated on Apple Silicon)

cd adapters/mq
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"

./.venv/bin/python scripts/seed.py                 # optional: sample data on DEV.QUEUE.1
./.venv/bin/python -m mq_adapter.main queues --pattern "DEV.*"
./.venv/bin/python -m mq_adapter.main peek DEV.QUEUE.1

./.venv/bin/pytest
```

Defaults assume the docker-compose queue manager (`QM1`, admin/adminpassw0rd, app/apppassw0rd, `https://localhost:9543`) — override with `--qmgr`/`--admin-url`/`--admin-username`/`--admin-password` (and `--app-username`/`--app-password` for `peek`) if yours differs. TLS verification is off by default (`verify_tls=False` in `MQAdapter`) since dev queue managers use self-signed certs — turn it on for anything real.

Note: IBM ships the MQ container images amd64-only; `docker-compose.yml` pins `platform: linux/amd64` so it runs under Rosetta/QEMU emulation on Apple Silicon. Works, but startup is slower than the Kafka/Solace images.

## Auth

Basic auth against both the REST Admin API (queue manager/queue monitoring — used with an admin-level user) and the REST Messaging API (message peek — used with an app-level user, matching MQ's usual admin/app credential separation). mTLS is supported by MQ's REST APIs but not wired up here.

## Next step

Decide how this CLI becomes a real service, same as the other two adapters — wire into the ingestion/poller described in [`/docs/architecture.md §4`](../../docs/architecture.md#4-architecture). All three broker adapters (Phase 1-3) are now feature-complete; the unified API layer and UI are next.
