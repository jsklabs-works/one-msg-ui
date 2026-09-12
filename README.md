# one-msg-ui

A single monitoring UI for heterogeneous messaging systems — starting with **IBM MQ**, **Apache Kafka**, and **Solace PubSub+**.

Each system is fundamentally different (point-to-point queues vs. distributed log vs. topic/queue hybrid), so rather than faking a one-size-fits-all abstraction, this project normalizes each broker's native metrics into a shared schema and builds the UI against that — see [`docs/architecture.md`](docs/architecture.md) for the full design.

## Status

Early scaffold. No adapters are implemented yet — this repo currently just holds the architecture doc and the planned directory layout.

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

Nothing runnable yet — this is the initial commit. Next step is the Kafka adapter (see `adapters/kafka/README.md`).
