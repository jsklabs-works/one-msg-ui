# IBM MQ adapter

**Status:** not started (Phase 3 — build after Kafka and Solace)

## Responsibility

Translate IBM MQ's native monitoring surface into the shared model defined in [`/docs/architecture.md`](../../docs/architecture.md#3-unified-data-model):

- `Resource` — one per queue, with `depth_current` (`CURDEPTH`) and `depth_max` (`MAXDEPTH`) populated; `consumer_lag` stays null (no per-consumer offset concept in MQ)
- `HealthEvent` — queue manager and channel status

## Data sources

- **REST Admin API** (`/ibmmq/rest/v1/admin/qmgr/{qmgr}/...`) — build against this first; covers queue depth, channel status, and queue manager status via plain JSON over HTTP
- **PCF (Programmatic Command Format)** — fallback only, for whatever specific attributes your MQ version doesn't yet expose over REST. Don't build full PCF support speculatively — add it only for the attributes you actually need that REST doesn't cover.

## Auth

Basic auth or bearer/OAuth (Cloud Pak deployments), with mTLS for transport. Confirm which auth model your queue managers are configured for before starting.

## Next step

Point the adapter at a test queue manager's REST Admin API and confirm which of the metrics this project needs are available there vs. requiring PCF.
