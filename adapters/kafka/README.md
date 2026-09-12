# Kafka adapter

**Status:** not started (Phase 1 — build this first)

## Responsibility

Translate Kafka's native monitoring surface into the shared model defined in [`/docs/architecture.md`](../../docs/architecture.md#3-unified-data-model):

- `Resource` — one per topic, with `consumer_lag` populated (Kafka has no native "depth" concept; `depth_current`/`depth_max` stay null)
- `ConsumerGroup` — group state, member count, and per-partition `{log_end_offset, committed_offset, lag}`
- `HealthEvent` — broker/ISR/under-replicated-partition issues
- `MessageSample` — non-destructive peek at a topic/partition's messages (see [`/docs/architecture.md §3.1`](../../docs/architecture.md#31-message-browsingpeek-in-scope-for-v1)): a scratch/no-commit consumer group or `assign()` + manual `seek()`, polling without ever committing offsets

## Data sources

- **Kafka Admin API** — offsets and consumer group metadata (primary source)
- **JMX** — broker internals, `records-lag-max` client-side metric, replication/ISR status
- Community exporters (`kafka-exporter`, JMX exporter) are worth evaluating as a starting point rather than writing the Prometheus-facing plumbing from scratch

## Known gotchas to handle

- Consumers using `assign()` instead of `subscribe()` don't report lag through the standard group-metadata path — fall back to computing lag directly (`log-end-offset - committed-offset`) via the Admin API rather than trusting a single source.
- Consumer groups that sit `EMPTY` for more than a day stop emitting lag metrics — the adapter should distinguish "no lag data" from "zero lag."

## Next step

Pick a client library (e.g. `kafka-python` / `confluent-kafka` for Python, or the Java `AdminClient`) and implement the offset-fetch + lag-calculation path against a local Kafka cluster before wiring up the exporter/Prometheus integration.
