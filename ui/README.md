# Dashboard UI

**Status:** MVP working — both required surfaces (see [architecture.md §7](../docs/architecture.md#7-decisions-resolved-2026-09-12)) are implemented and verified end-to-end against the real API layer, which itself is verified against the three real local dev brokers. React + Vite + TypeScript.

## Responsibility

Cross-system dashboard and per-resource drill-down — see [`/docs/architecture.md`](../docs/architecture.md#4-architecture) for where this sits in the overall pipeline. Alerting is explicitly not this UI's job — it's a read-only operations/support view alongside each broker's native alerting.

Two surfaces, both first-class per the resolved usage-pattern decision:

- **Overview** ([`src/pages/Overview.tsx`](src/pages/Overview.tsx)) — a stats summary (brokers online, resources monitored, open health issues, total Kafka consumer lag — see [`components/StatsSummary.tsx`](src/components/StatsSummary.tsx)), a broker-status strip, and a table of every resource across every broker, filterable by environment and system type. The "what's on fire" screen. When zero brokers are configured, this becomes a landing page inviting you to add one instead of an empty table.
- **Drill-down** ([`src/pages/ResourceDetail.tsx`](src/pages/ResourceDetail.tsx)) — narrow, deep, single-resource: full stats, broker health for that resource's broker, Kafka consumer-group/partition-lag breakdown when applicable, and non-destructive message browsing (architecture.md §3.1). The "let me actually look at this" screen. Message bodies are shown as the API's already-truncated `body_preview`, never a raw full payload.

### Adding a broker

[`components/AddBrokerForm.tsx`](src/components/AddBrokerForm.tsx) is reachable from the "+ Add broker" button (always visible) or the empty-state landing page (zero brokers configured). It fetches `GET /api/system-types` and renders whatever fields come back — the form has no hardcoded knowledge of what Kafka vs. Solace vs. MQ need, including their real TLS/SASL options (see [api/README.md's TLS/SASL section](../api/README.md#tlssasl)). Submitting actually tries to connect (`POST /api/brokers`) before anything is saved; a bad host/port/credential shows the real connection error inline rather than silently saving a broken broker. Each broker card also has a "Remove" action (with an inline yes/no confirm, not a native `confirm()` dialog — those don't play well with automated browser testing and are generally worse UX) so the empty state is actually reachable through the UI, not just by hand-editing `brokers.json`.

## Layout

```
src/
  api.ts                     typed fetch client — mirrors api/src/api/models.py by hand, keep in sync
  healthUtils.ts             pure helpers (worst-severity-among-events, filter-by-broker)
  index.css                  design tokens — the dark-mode values from the dataviz skill's
                              validated reference palette (status colors, the first 3
                              all-pairs-validated categorical hues for the 3 system types)
  components/Badges.tsx      SystemTypeBadge / SeverityBadge / BrokerStatusBadge
  components/StatsSummary.tsx  the stat-tile row (brokers online, resources, health issues,
                              consumer lag) — derived client-side from data already fetched
  components/AddBrokerForm.tsx  dynamic add-broker form, driven by GET /api/system-types
  pages/Overview.tsx         the broad cross-broker view + empty-state landing page
  pages/ResourceDetail.tsx   the per-resource drill-down + message peek
  App.tsx                    react-router routes: "/" and "/resources/:resourceId"
```

## Visual design

Dark-committed by design (an ops console, not a marketing page) rather than a light/dark toggle — see `index.css`'s header comment. Colors aren't ad-hoc: they're the dark-mode token values from the `dataviz` skill's validated reference palette — status colors (good/warning/critical) for severity and broker-status badges, and the first three categorical hues (the specific trio the palette's validator confirms clears its CVD/contrast checks together) for the three system-type badges. No new hex values were invented, so no re-validation was needed — see the skill's `references/palette.md` if extending this to a 4th system type or a light mode.

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

Verified end-to-end in a real browser against the live API and all three real local brokers: the resource table loads and filters correctly, drill-down renders correct fields per system type (including the `n/a for this system` cases — Kafka has no depth, Solace/MQ have no consumer lag), Kafka's consumer-group/partition table renders, and message peek works and displays correctly for all three systems (including MQ's single-message limitation, surfaced as an explicit note in the UI rather than silently capping the limit input). Also verified live: removing all three brokers correctly reaches the empty-state landing page, and adding a broker back through the form (including its TLS/SASL fields) performs a real connection test and immediately shows live data — both the success path (Solace, valid config) and the failure path (Kafka, unreachable host, error shown inline without corrupting the saved config) were exercised in a real browser, not just unit-tested.

Not done: auth/access-control (architecture.md §7 flags per-environment access control as a first-class requirement, not yet built), loading states beyond a basic spinner-less "Loading…"/"Refreshing…", and the periodic 15s poll on Overview hasn't been tested against a broker actually going down mid-session (the aggregator's graceful-degradation path is unit-tested in `api/tests/test_aggregator.py`, but not exercised live here).

## Next step

Real auth/access control per environment (architecture.md §7). Longer-term, once the API layer moves off the live-passthrough MVP to the poller+store design, the UI's polling can drop to reading from that store instead of triggering live broker queries every 15s.
