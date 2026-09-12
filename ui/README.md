# Dashboard UI

**Status:** not started — build once the API layer has at least one adapter's data flowing through it

## Responsibility

Cross-system dashboard and per-broker/per-resource drill-down — see [`/docs/architecture.md`](../docs/architecture.md#4-architecture) for where this sits in the overall pipeline. Alerting is explicitly not this UI's job (see [architecture.md §7](../docs/architecture.md#7-decisions-resolved-2026-09-12)) — it's a read-only operations/support view alongside each broker's native alerting.

Two surfaces are both first-class from Phase 1 onward, per the resolved usage-pattern decision — don't build one and defer the other:

- **Overview** — broad, shallow, cross-broker: many brokers/resources at a glance, filterable by `environment` (prod/uat/dev) and `system_type`, the "what's on fire" screen.
- **Drill-down** — narrow, deep, single-resource: full detail on one queue/topic including consumer-group partition breakdown (Kafka) and the message-browsing view (`MessageSample`, see [architecture.md §3.1](../docs/architecture.md#31-message-browsingpeek-in-scope-for-v1)) — the "let me actually look at this" screen a support engineer or on-call person uses mid-investigation. Message bodies should default to a truncated preview, not full payload, given they may carry sensitive data.

Tech stack not yet chosen — this is a conventional dashboard/table/drill-down app, so use whatever your team's standard web stack is rather than let the messaging domain drive that choice.
