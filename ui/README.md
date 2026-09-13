# Dashboard UI

**Status:** MVP working — both required surfaces (see [architecture.md §7](../docs/architecture.md#7-decisions-resolved-2026-09-12)) are implemented and verified end-to-end against the real API layer, which itself is verified against the five real local dev brokers. React + Vite + TypeScript.

## Responsibility

A tabbed dashboard: one **Dashboard** tab plus one tab per connected broker, each a real route — see [`/docs/architecture.md`](../docs/architecture.md#4-architecture) for where this sits in the overall pipeline. Alerting is explicitly not this UI's job — it's a read-only operations/support view alongside each broker's native alerting.

**Logo:** a hub-and-spoke mark — three nodes converging on one — in the app's own categorical trio (`--cat-blue`/`--cat-orange`/`--cat-aqua` from `index.css`, the same three colors every `SystemTypeBadge` uses for Kafka/MQ/Solace), on a rounded gradient tile. It's meant to say the one thing this whole app does: three different messaging systems, one view. [`components/Logo.tsx`](src/components/Logo.tsx) exports `LogoGlyph` (the mark alone, for the header's `.brand-mark` tile, which paints its own matching gradient) and `LogoMark` (the same mark with its own gradient tile baked in, for the empty-state landing page). [`public/favicon.svg`](public/favicon.svg) is the same design again for the browser tab — kept in sync by hand since it's a static file, not generated from the component.

- **Dashboard tab** (`/`, [`src/pages/Dashboard.tsx`](src/pages/Dashboard.tsx)) — the stats summary (brokers online, resources monitored, open health issues, total Kafka consumer lag — see [`components/StatsSummary.tsx`](src/components/StatsSummary.tsx)) plus a table with **one row per broker**, filterable by environment and system type. A row shows that broker's object count and kind ("12 topics", "2 queues across 2 VPNs" — only mentions namespaces when there's more than one), total consumer lag, and worst health severity; the broker name links straight to its own tab. This is deliberately a summary, not a flattened list of every topic/queue/namespace across every broker — a real deployment can have hundreds of those, and a giant flat table stops being glanceable well before then (see the git history for the version that listed one row per object; it didn't scale). No per-object drill-down from here — that lives on each broker's own tab now. The "what's on fire" screen. When zero brokers are configured, this becomes a landing page inviting you to add one instead of an empty table.
- **Per-broker tab** (`/brokers/:brokerId`, [`src/pages/BrokerView.tsx`](src/pages/BrokerView.tsx)) — one tab per configured broker (see [`components/Layout.tsx`](src/components/Layout.tsx)'s tab bar, with a status dot per tab), showing that broker's own health and resources grouped by namespace — the real hierarchy, not a flattened column (Broker → VPN → queues for Solace, Broker → queue manager → queues for MQ, Broker → cluster → topics for Kafka; labels come from [`src/labels.ts`](src/labels.ts)) — each one with an "Inspect →" link, plus a "Remove this broker" action. This is where the actual per-object list lives (the Dashboard only summarizes counts). The section heading and column say "Topics"/"Queues", never the internal "Resource" model name — `labels.ts`'s `kindLabel()` reads the real word off each resource's own `kind` field rather than the UI guessing from system type. Because tabs are real routes rather than client-side-only tab state, opening a second real browser tab/window on a different broker's URL lets you inspect two brokers side by side — a single "selected tab" variable never could.
- **Resource drill-down** (`/resources/:resourceId`, [`src/pages/ResourceDetail.tsx`](src/pages/ResourceDetail.tsx)) — narrow, deep, single-resource: full stats, broker health, Kafka consumer-group/partition-lag breakdown when applicable, and non-destructive message browsing (architecture.md §3.1). Reached by clicking "Inspect" from a broker's own tab; the back-link returns to that same broker's tab. Each peeked message shows its actual topic/destination (a highlighted badge — for Solace this can genuinely differ from the queue being browsed, if the queue has a topic subscription) plus two clearly labeled sections: **Headers** (Kafka record headers, Solace message properties, MQ's `ibm-mq-md-*` headers — a plain "No headers on this message" when there are none, not a blank space) and **Payload**, each message body shown as the API's already-truncated `body_preview`, never a raw full payload. A **Download** link next to each payload saves it to a file via a plain `Blob`/`<a download>` (works in any real browser — no server round trip); if the preview itself was truncated, the button says so ("Download (truncated)") rather than silently handing over an incomplete file with no indication.

  If a peek fails with what looks like a credentials problem (checked by matching the error text against `incorrect`/`unauthorized`/`login`/`credential`/`auth`/`401`/`403` — see `looksLikeAuthError` in the same file), an inline form appears instead of a dead-end error banner: username + password, retried via `POST /api/resources/{id}/messages` (a password has no business in a URL, so this isn't the same `GET ...?limit=` the plain peek uses). Nothing entered here is saved — it's a one-off retry, since the real fix for a recurring case is on the broker itself (see [api/README.md's per-resource peek credentials section](../api/README.md#per-resource-peek-credentials)). Verified live against a real auth failure: the prompt appeared, a wrong retry showed the same error and left the form open for another attempt, and the same POST endpoint verified separately with correct credentials against a working queue.

All three pages read from one shared poll ([`context/MonitoringDataContext.tsx`](src/context/MonitoringDataContext.tsx)) rather than each fetching independently — switching tabs is instant, never a fresh network round trip, and there's exactly one 15s refresh timer for the whole app.

### Adding a broker

[`components/AddBrokerForm.tsx`](src/components/AddBrokerForm.tsx) is reachable from the "+ Add broker" button in the header (always visible, on every tab) or the empty-state landing page (zero brokers configured). It fetches `GET /api/system-types` and renders whatever fields come back — the form has no hardcoded knowledge of what Kafka vs. Solace vs. MQ need, including their real TLS/SASL options (see [api/README.md's TLS/SASL section](../api/README.md#tlssasl)). Submitting actually tries to connect (`POST /api/brokers`) before anything is saved; a bad host/port/credential shows the real connection error inline rather than silently saving a broken broker. On success it navigates straight to the new broker's tab. Each broker's own tab has a "Remove this broker" action (inline yes/no confirm, not a native `confirm()` dialog — those don't play well with automated browser testing and are generally worse UX) so the empty state is actually reachable through the UI, not just by hand-editing `brokers.json`.

Next to it, "Import JSON" ([`components/ImportBrokersForm.tsx`](src/components/ImportBrokersForm.tsx)) adds several brokers at once from a file shaped like [`api/config/brokers.json`](../api/config/brokers.json) itself (a top-level `brokers` array), rather than filling the single-broker form once per entry — handy for standing up a fresh instance from an existing inventory. It's client-side JSON parsing plus one call to `POST /api/brokers/import`; the API runs the exact same validation/duplicate-name/duplicate-connection/connection-test checks per entry as the single-add form (see [api/README.md's duplicate-connection section](../api/README.md#duplicate-connection-detection)), and one bad entry never blocks the rest — the response is a per-entry result list, rendered as a status per broker ("Added" / "Skipped — ..." with the real reason) rather than one pass/fail for the whole file. Verified live with a 3-entry file where two entries duplicated already-configured brokers and one had a wrong password: each got its own correct status and detail, nothing was added, and the dashboard's broker list was untouched.

"Export JSON" is the other half of that round trip: one `GET /api/brokers/export` call, saved as a `brokers.json` download via a plain `Blob`/`<a download>` (same technique as the message-payload download in the resource drill-down — see below). It's exactly what `POST /api/brokers/import` accepts, so exporting from one instance and importing into another (or just backing up the current inventory) needs no reshaping — verified by round-tripping a real export through the import endpoint in [`api/tests/test_main.py`](../api/tests/test_main.py). The export includes each broker's full config, credentials included — the button's tooltip says so, since that's the same plaintext-in-JSON shape `config/brokers.json` already is on disk (not new exposure), but a downloaded file is now something a person can carry around or email.

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
  components/Logo.tsx       the app's logo — LogoGlyph (header brand-mark) and LogoMark
                              (self-contained, for the empty-state landing page); same mark
                              as public/favicon.svg, kept in sync by hand (see below)
  components/Badges.tsx      SystemTypeBadge / SeverityBadge / BrokerStatusBadge
  components/StatsSummary.tsx  the stat-tile row (brokers online, resources, health issues,
                              consumer lag) — derived client-side from data already fetched
  components/AddBrokerForm.tsx  dynamic add-broker form, driven by GET /api/system-types
  components/ImportBrokersForm.tsx  bulk add from a brokers.json-shaped file, one result per entry
  labels.ts                  NAMESPACE_LABELS (VPN/Queue manager/Cluster), kindLabel()
                              (Topic/Queue, read off resource.kind), and namespaceLabelPlural()
                              (mid-sentence plural, "VPNs"/"queue managers") — the UI's
                              product-specific vocabulary, so no page shows the internal
                              "Resource" model name
  pages/Dashboard.tsx        the "/" tab: stats + read-only cross-broker table
  pages/BrokerView.tsx       the "/brokers/:brokerId" tab: one broker's health/resources/remove
  pages/ResourceDetail.tsx   the "/resources/:resourceId" drill-down + message peek
  App.tsx                    react-router routes, nested under Layout
```

## Visual design

Colors aren't ad-hoc: every token is the `dataviz` skill's validated reference palette — status colors (good/warning/critical) for severity and broker-status badges, and the fixed categorical hue order for the system-type badges: the first three (the trio the palette's validator confirms clears its CVD/contrast checks together, all-pairs) for Kafka/Solace/MQ, slot 4 (yellow) for RabbitMQ, then slot 5 (magenta) for ActiveMQ — safe for badges specifically because they're always adjacent in a table column, not the all-pairs case (scatter/bubble) that caps at three; see the skill's `references/palette.md`. Slots 4 and 5 are both among the palette's "sits below 3:1 on light" hues, so `--badge-rabbitmq-fg`/`--badge-activemq-fg` are darkened mustard/rose rather than the raw categorical values — same trick `--badge-warn-fg` already uses for the same underlying hue family.

**Theme:** light, dark, or system (follows the OS live, including if it changes while the page is open) — the toggle in the header, backed by [`useTheme.ts`](src/useTheme.ts). Light is the CSS base; dark is layered on via `prefers-color-scheme` (for "system") and a `[data-theme="dark"]` attribute (for an explicit choice), both reading from the same token set so nothing is duplicated. The choice persists in `localStorage`, and an inline script in `index.html` applies it before first paint so there's no flash of the wrong theme on reload. Badge background/foreground pairs are tuned per mode (not the same hex reused on both a light and a dark surface) — see `index.css`'s `--badge-*` tokens.

## Running it locally

Needs the [unified API layer](../api/README.md) running first (which itself needs the five adapters' dev brokers up).

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

Verified end-to-end in a real browser against the live API and all three original real local brokers: the Dashboard tab's stats and resource table load and filter correctly (with no Inspect link, by design), each broker's own tab shows its health/resources correctly with a working "Inspect →", the resource drill-down renders correct fields per system type (including the `n/a for this system` cases) with a back-link to the right broker tab, Kafka's consumer-group/partition table renders, and message peek works for all three systems (including MQ's single-message limitation, surfaced as an explicit UI note). Also verified live: two genuinely separate browser tabs opened on two different `/brokers/:id` URLs at once, each showing that broker's own data independently — the actual "inspect multiple brokers simultaneously" requirement, not just a UI claim; removing all three brokers reaches the empty-state landing page (tabs disappear along with them); adding a broker back through the form (TLS/SASL fields included) performs a real connection test, navigates straight to its new tab, and shows live data — both the success path (Solace) and failure path (Kafka, unreachable host, inline error) were exercised live, not just unit-tested.

RabbitMQ added later, verified the same way against a real broker (Docker): the Dashboard row and broker tab show correct vhost/queue grouping ("Vhost: /", "Vhost: tenant-a"), the badge renders in its own distinct color, peek returns real headers/payload/topic (routing key) and is confirmed non-destructive (queue depth unchanged across repeated peeks), and — a real bug this surfaced, not just a happy-path check — the default vhost's literal name `"/"` initially broke `/api/resources/{resource_id}` routing (a raw `/` inside a path segment doesn't survive Starlette's routing even percent-encoded); fixed by escaping just that one well-known value in the adapter's id scheme (see [`adapters/rabbitmq/README.md`](../adapters/rabbitmq/README.md)) and re-verified live.

ActiveMQ Artemis added the same way after that: the Dashboard row and broker tab show correct address/queue grouping ("Address: orders"), the badge renders in its own distinct color, and peek returns real headers/payload/topic (the message's address) and is confirmed non-destructive. This one also surfaced a real bug in `labels.ts` rather than the API: pluralizing "Address" the same naive way as the other namespace labels produced "4 addresss" (a double s) on the Dashboard — `namespaceLabelPlural()` now checks for a trailing s/x/z/ch/sh and adds "es" instead of a bare "s" for those.

Not done: auth/access-control (architecture.md §7 flags per-environment access control as a first-class requirement, not yet built), loading states beyond a basic spinner-less "Loading…"/"Refreshing…", and the periodic 15s poll in `MonitoringDataContext` hasn't been tested against a broker actually going down mid-session (the aggregator's graceful-degradation path is unit-tested in `api/tests/test_aggregator.py`, but not exercised live here).

## Next step

Real auth/access control per environment (architecture.md §7). Longer-term, once the API layer moves off the live-passthrough MVP to the poller+store design, the UI's polling can drop to reading from that store instead of triggering live broker queries every 15s.
