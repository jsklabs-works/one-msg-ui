# Dashboard UI

**Status:** MVP working — both required surfaces (see [architecture.md §7](../docs/architecture.md#7-decisions-resolved-2026-09-12)) are implemented and verified end-to-end against the real API layer, which itself is verified against the three real local dev brokers. React + Vite + TypeScript.

## Responsibility

Cross-system dashboard and per-resource drill-down — see [`/docs/architecture.md`](../docs/architecture.md#4-architecture) for where this sits in the overall pipeline. Alerting is explicitly not this UI's job — it's a read-only operations/support view alongside each broker's native alerting.

Two surfaces, both first-class per the resolved usage-pattern decision:

- **Overview** ([`src/pages/Overview.tsx`](src/pages/Overview.tsx)) — broad, shallow, cross-broker: a broker-status strip plus a table of every resource across every broker, filterable by environment and system type. The "what's on fire" screen.
- **Drill-down** ([`src/pages/ResourceDetail.tsx`](src/pages/ResourceDetail.tsx)) — narrow, deep, single-resource: full stats, broker health for that resource's broker, Kafka consumer-group/partition-lag breakdown when applicable, and non-destructive message browsing (architecture.md §3.1). The "let me actually look at this" screen. Message bodies are shown as the API's already-truncated `body_preview`, never a raw full payload.

## Layout

```
src/
  api.ts               typed fetch client — mirrors api/src/api/models.py by hand, keep in sync
  healthUtils.ts        pure helpers (worst-severity-among-events, filter-by-broker)
  components/Badges.tsx  SystemTypeBadge / SeverityBadge / BrokerStatusBadge
  pages/Overview.tsx     the broad cross-broker view
  pages/ResourceDetail.tsx  the per-resource drill-down + message peek
  App.tsx                react-router routes: "/" and "/resources/:resourceId"
```

## Running it locally

Needs the [unified API layer](../api/README.md) running first (which itself needs the three adapters' dev brokers up).

```bash
cd api && ./.venv/bin/uvicorn api.main:app --port 8010   # in one terminal

cd ui
npm install
cp .env.example .env    # VITE_API_BASE_URL, defaults to http://localhost:8010
npm run dev             # opens on :5173
```

Or via this repo's `.claude/launch.json` config `ui-dev` if you're driving this through Claude Code's preview tooling.

`npm run build` produces a static `dist/` bundle; `npx tsc --noEmit` type-checks without building.

## What's verified vs. what's not

Verified end-to-end in a real browser against the live API and all three real local brokers: the resource table loads and filters correctly, drill-down renders correct fields per system type (including the `n/a for this system` cases — Kafka has no depth, Solace/MQ have no consumer lag), Kafka's consumer-group/partition table renders, and message peek works and displays correctly for all three systems (including MQ's single-message limitation, surfaced as an explicit note in the UI rather than silently capping the limit input).

Not done: auth/access-control (architecture.md §7 flags per-environment access control as a first-class requirement, not yet built), loading states beyond a basic spinner-less "Loading…"/"Refreshing…", and the periodic 15s poll on Overview hasn't been tested against a broker actually going down mid-session (the aggregator's graceful-degradation path is unit-tested in `api/tests/test_aggregator.py`, but not exercised live here).

## Next step

Real auth/access control per environment (architecture.md §7). Longer-term, once the API layer moves off the live-passthrough MVP to the poller+store design, the UI's polling can drop to reading from that store instead of triggering live broker queries every 15s.
