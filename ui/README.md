# Dashboard UI

**Status:** MVP working — both required surfaces (see [architecture.md §7](../docs/architecture.md#7-decisions-resolved-2026-09-12)) are implemented and verified end-to-end against the real API layer, which itself is verified against the three real local dev brokers. React + Vite + TypeScript.

## Responsibility

A tabbed dashboard: one **Dashboard** tab plus one tab per connected broker, each a real route — see [`/docs/architecture.md`](../docs/architecture.md#4-architecture) for where this sits in the overall pipeline. Alerting is explicitly not this UI's job — it's a read-only operations/support view alongside each broker's native alerting.

- **Dashboard tab** (`/`, [`src/pages/Dashboard.tsx`](src/pages/Dashboard.tsx)) — the stats summary (brokers online, resources monitored, open health issues, total Kafka consumer lag — see [`components/StatsSummary.tsx`](src/components/StatsSummary.tsx)) plus a read-only table of every resource across every broker, filterable by environment and system type. No per-resource drill-down from here — that lives on each broker's own tab now. The "what's on fire" screen. When zero brokers are configured, this becomes a landing page inviting you to add one instead of an empty table.
- **Per-broker tab** (`/brokers/:brokerId`, [`src/pages/BrokerView.tsx`](src/pages/BrokerView.tsx)) — one tab per configured broker (see [`components/Layout.tsx`](src/components/Layout.tsx)'s tab bar, with a status dot per tab), showing that broker's own health and resources grouped by namespace — the real hierarchy, not a flattened column (Broker → VPN → queues for Solace, Broker → queue manager → queues for MQ, Broker → cluster → topics for Kafka; labels come from [`src/labels.ts`](src/labels.ts)) — each resource with an "Inspect →" link, plus a "Remove this broker" action. Because tabs are real routes rather than client-side-only tab state, opening a second real browser tab/window on a different broker's URL lets you inspect two brokers side by side — a single "selected tab" variable never could.
- **Resource drill-down** (`/resources/:resourceId`, [`src/pages/ResourceDetail.tsx`](src/pages/ResourceDetail.tsx)) — narrow, deep, single-resource: full stats, broker health, Kafka consumer-group/partition-lag breakdown when applicable, and non-destructive message browsing (architecture.md §3.1). Reached by clicking "Inspect" from a broker's own tab; the back-link returns to that same broker's tab. Each peeked message shows its actual topic/destination (a highlighted badge — for Solace this can genuinely differ from the queue being browsed, if the queue has a topic subscription) plus its properties (Kafka record headers, Solace message properties, MQ's `ibm-mq-md-*` headers) as a key/value list — these were already being fetched by the API but were silently dropped by the UI before. Message bodies are shown as the API's already-truncated `body_preview`, never a raw full payload.

All three pages read from one shared poll ([`context/MonitoringDataContext.tsx`](src/context/MonitoringDataContext.tsx)) rather than each fetching independently — switching tabs is instant, never a fresh network round trip, and there's exactly one 15s refresh timer for the whole app.

### Adding a broker

[`components/AddBrokerForm.tsx`](src/components/AddBrokerForm.tsx) is reachable from the "+ Add broker" button in the header (always visible, on every tab) or the empty-state landing page (zero brokers configured). It fetches `GET /api/system-types` and renders whatever fields come back — the form has no hardcoded knowledge of what Kafka vs. Solace vs. MQ need, including their real TLS/SASL options (see [api/README.md's TLS/SASL section](../api/README.md#tlssasl)). Submitting actually tries to connect (`POST /api/brokers`) before anything is saved; a bad host/port/credential shows the real connection error inline rather than silently saving a broken broker. On success it navigates straight to the new broker's tab. Each broker's own tab has a "Remove this broker" action (inline yes/no confirm, not a native `confirm()` dialog — those don't play well with automated browser testing and are generally worse UX) so the empty state is actually reachable through the UI, not just by hand-editing `brokers.json`.

## Layout

```
src/
  api.ts                     typed fetch client — mirrors api/src/api/models.py by hand, keep in sync
  useTheme.ts                light/dark/system preference — persisted to localStorage, applied
                              as a data-theme attribute; see index.html's inline bootstrap script
  healthUtils.ts             pure helpers (worst-severity-among-events, filter-by-broker)
  index.css                  design tokens for all three theme states — the dataviz skill's
                              validated reference palette (light values as the base, dark
                              values layered on via prefers-color-scheme + [data-theme])
  context/MonitoringDataContext.tsx  the one shared poll — brokers/resources/health/
                              consumerGroups — every page reads from this, none fetch alone
  components/Layout.tsx      header + tab bar (Dashboard + one tab per broker) + <Outlet/>
  components/ThemeToggle.tsx  the light/dark/system icon-only segmented control in the header
  components/Badges.tsx      SystemTypeBadge / SeverityBadge / BrokerStatusBadge
  components/StatsSummary.tsx  the stat-tile row (brokers online, resources, health issues,
                              consumer lag) — derived client-side from data already fetched
  components/AddBrokerForm.tsx  dynamic add-broker form, driven by GET /api/system-types
  labels.ts                  NAMESPACE_LABELS — VPN / Queue manager / Cluster, per system type
  pages/Dashboard.tsx        the "/" tab: stats + read-only cross-broker table
  pages/BrokerView.tsx       the "/brokers/:brokerId" tab: one broker's health/resources/remove
  pages/ResourceDetail.tsx   the "/resources/:resourceId" drill-down + message peek
  App.tsx                    react-router routes, nested under Layout
```

## Visual design

Colors aren't ad-hoc: every token is the `dataviz` skill's validated reference palette — status colors (good/warning/critical) for severity and broker-status badges, and the first three categorical hues (the specific trio the palette's validator confirms clears its CVD/contrast checks together) for the three system-type badges. No new hex values were invented in either mode, so no re-validation was needed — see the skill's `references/palette.md` if extending this to a 4th system type.

**Theme:** light, dark, or system (follows the OS live, including if it changes while the page is open) — the toggle in the header, backed by [`useTheme.ts`](src/useTheme.ts). Light is the CSS base; dark is layered on via `prefers-color-scheme` (for "system") and a `[data-theme="dark"]` attribute (for an explicit choice), both reading from the same token set so nothing is duplicated. The choice persists in `localStorage`, and an inline script in `index.html` applies it before first paint so there's no flash of the wrong theme on reload. Badge background/foreground pairs are tuned per mode (not the same hex reused on both a light and a dark surface) — see `index.css`'s `--badge-*` tokens.

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

Verified end-to-end in a real browser against the live API and all three real local brokers: the Dashboard tab's stats and resource table load and filter correctly (with no Inspect link, by design), each broker's own tab shows its health/resources correctly with a working "Inspect →", the resource drill-down renders correct fields per system type (including the `n/a for this system` cases) with a back-link to the right broker tab, Kafka's consumer-group/partition table renders, and message peek works for all three systems (including MQ's single-message limitation, surfaced as an explicit UI note). Also verified live: two genuinely separate browser tabs opened on two different `/brokers/:id` URLs at once, each showing that broker's own data independently — the actual "inspect multiple brokers simultaneously" requirement, not just a UI claim; removing all three brokers reaches the empty-state landing page (tabs disappear along with them); adding a broker back through the form (TLS/SASL fields included) performs a real connection test, navigates straight to its new tab, and shows live data — both the success path (Solace) and failure path (Kafka, unreachable host, inline error) were exercised live, not just unit-tested.

Not done: auth/access-control (architecture.md §7 flags per-environment access control as a first-class requirement, not yet built), loading states beyond a basic spinner-less "Loading…"/"Refreshing…", and the periodic 15s poll in `MonitoringDataContext` hasn't been tested against a broker actually going down mid-session (the aggregator's graceful-degradation path is unit-tested in `api/tests/test_aggregator.py`, but not exercised live here).

## Next step

Real auth/access control per environment (architecture.md §7). Longer-term, once the API layer moves off the live-passthrough MVP to the poller+store design, the UI's polling can drop to reading from that store instead of triggering live broker queries every 15s.
