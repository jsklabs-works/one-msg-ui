# Unified API layer

**Status:** not started — build once at least one adapter (Kafka) is producing real data

## Responsibility

Serves the normalized model (`Broker`, `Resource`, `ConsumerGroup`, `HealthEvent`, `MessageSample` — see [`/docs/architecture.md`](../docs/architecture.md#3-unified-data-model)) to the UI, reading from whatever metrics/metadata store the adapters write into.

Not yet decided: REST vs. GraphQL. GraphQL is worth it if the UI needs cross-cutting queries ("all resources across all brokers above 80% depth") often — revisit once the UI's actual query patterns are clearer.

## Decisions carried from the architecture doc (see [`/docs/architecture.md §7`](../docs/architecture.md#7-decisions-resolved-2026-09-12))

- **Environment scoping is first-class from day one:** prod/uat/dev all in scope; every query is implicitly or explicitly scoped by `Broker.environment`, and access control must account for it (a caller scoped to uat/dev must not implicitly see prod).
- **Credentials:** broker credentials are stored encrypted in the app's own Postgres metadata store (no external vault dependency) — this layer is what reads them to authenticate adapter calls, so treat that read path as sensitive and never let credentials leak into logs or API responses.
- **No alerting:** this layer never exposes alert-rule CRUD or notification config — it's a read/query surface over normalized state, full stop.
- **Message browsing:** `MessageSample` responses should default to a truncated `body_preview`, with full-body access gated the same way as everything else (per environment/access control), since payloads may carry sensitive data.
