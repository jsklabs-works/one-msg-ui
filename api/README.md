# Unified API layer

**Status:** not started — build once at least one adapter (Kafka) is producing real data

## Responsibility

Serves the normalized model (`Broker`, `Resource`, `ConsumerGroup`, `HealthEvent` — see [`/docs/architecture.md`](../docs/architecture.md#3-unified-data-model)) to the UI, reading from whatever metrics/metadata store the adapters write into.

Not yet decided: REST vs. GraphQL. GraphQL is worth it if the UI needs cross-cutting queries ("all resources across all brokers above 80% depth") often — revisit once the UI's actual query patterns are clearer.
