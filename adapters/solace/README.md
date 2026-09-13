# Solace PubSub+ adapter

**Status:** Phase 2 complete — VPN/queue monitoring (SEMP v2), connectivity + spool-usage health, and non-destructive message browsing are all implemented and verified against a real Solace PubSub+ broker.

## Responsibility

Translate Solace's native monitoring surface into the shared model defined in [`/docs/architecture.md`](../../docs/architecture.md#3-unified-data-model):

- `Resource` — one per queue (spooled messages/bytes → `depth_current`; configured spool quota → `depth_max`)
- Solace is scoped two levels deep (**Message VPN → queue/topic**) — kept explicit as `Resource.namespace` rather than flattened away, per the architecture doc's warning. **A single adapter/broker connection spans every VPN its credentials can see, discovered dynamically** — `vpn_name` is an optional filter to pin to one VPN, not a required scope. (The first version of this adapter got this wrong: it hardcoded one VPN per connection, so a VPN created on the broker afterward silently never showed up. Verified live: creating a new VPN and re-fetching with no config change picks it up immediately.)
- `HealthEvent` — VPN connectivity (`state`/`enabled`) and spool-usage capacity, classified against the VPN's *own* configured `eventMsgSpoolUsageThreshold` rather than a value this adapter invents. **Real units bug found live, now fixed:** the VPN monitor object's `msgSpoolUsage` field is in *bytes* — confirmed empirically, it equals the exact sum of the VPN's queues' `spooledByteCount` — while `maxMsgSpoolUsage` sitting right next to it is in *MB*, matching the admin console's "Configured Quota". Comparing them directly (as the first version of this code did) reported a VPN with 506 bytes spooled against a 100 MB quota as **506% full / critical**, when the broker's own console showed 0.0005 MB used. See [`client.py`](src/solace_adapter/client.py)'s spool-usage block and the regression test in [`tests/test_client.py`](tests/test_client.py).
- `MessageSample` — non-destructive peek at a queue's messages (see [`/docs/architecture.md §3.1`](../../docs/architecture.md#31-message-browsingpeek-in-scope-for-v1)), including `topic`: the message's actual destination via `InboundMessage.get_destination_name()`, which can genuinely differ from the queue being browsed — a message published to a topic and spooled onto this queue via a topic subscription reports that topic, not the queue name

## Data sources

- **SEMP v2 monitor API** (`GET /SEMP/v2/monitor/...`) — JSON over REST, used for everything except message bodies (queues, VPN health, spool usage)
- **Official `solace-pubsubplus` Python client** — used only for message browsing (see below). Installs cleanly via pip on this platform (prebuilt wheel, no native build step needed).

## Message browsing: what we found

The architecture doc flagged this as Solace's hard case, assuming SEMP couldn't do it. **Verified empirically against a real broker:**

- SEMP v2 monitor *does* have queue message-inspection endpoints (`GET /queues/{q}/msgs` and `/queues/{q}/msgs/{msgId}`) — but confirmed they return metadata only (`msgId`, `attachmentSize`, `spooledTime`, `replicationGroupMsgId`, redelivery count, etc.), **never the payload**. So the original assumption was right, just for a more specific reason than "SEMP can't do this at all."
- The official `solace-pubsubplus` client's `MessageQueueBrowser` (`solace.messaging.receiver.queue_browser`) is a real, broker-native, non-destructive browse — not a workaround. Messages are read oldest-to-newest and remain on the queue (confirmed: spooled count was unchanged after browsing all 4 test messages). It even exposes `browser.remove()` for selective deletion, which this adapter deliberately never calls.
- This needs a separate connection (SMF, via the messaging client) alongside the SEMP monitoring connection — two connection types for one adapter, as flagged. See [`peek.py`](src/solace_adapter/peek.py).
- **Multi-VPN caveat found while testing VPN auto-discovery:** SEMP admin credentials work broker-wide across every VPN (that's what makes discovery possible at all), but an SMF connection for peek is authenticated *per VPN* — a username valid for SEMP monitoring is not automatically authorized as an SMF client on every VPN. Verified live: peek worked immediately on the `default` VPN but failed with `SOLCLIENT_SUBCODE_LOGIN_FAILURE` on a freshly created VPN, because that VPN hadn't been configured to accept that client username. This is the broker's own per-VPN client-authentication configuration, not something the adapter can paper over — the error is surfaced as-is (see `aggregator.peek()` in the API layer) rather than swallowed.

## Layout

```
src/solace_adapter/
  models.py   normalized dataclasses (Resource, HealthEvent, MessageSample)
  client.py   SolaceAdapter — SEMP v2 queue/VPN discovery, health
  health.py   pure health-classification helpers (unit-tested, no broker needed)
  peek.py     non-destructive message browsing via solace-pubsubplus's MessageQueueBrowser
  main.py     CLI entry point (`solace-adapter queues` / `solace-adapter peek <queue>`)
scripts/seed.py   dev-only: creates a test queue + publishes messages via SEMP config + REST messaging
tests/            pytest suite for health.py, peek.py, and client.py's pure/mockable logic
```

## Running it locally

This project doesn't run its own Solace broker via docker-compose — point it at whatever broker you have. Defaults below assume a Solace PubSub+ broker (Docker or otherwise) reachable at `localhost` with admin/admin; override with CLI flags if yours differs.

```bash
cd adapters/solace
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"

# optional: seed a queue with test messages
./.venv/bin/python scripts/seed.py

./.venv/bin/python -m solace_adapter.main queues                       # discovers every VPN
./.venv/bin/python -m solace_adapter.main --vpn default queues          # or just one
./.venv/bin/python -m solace_adapter.main --vpn default peek one-msg-ui-test --limit 5

./.venv/bin/pytest
```

Flags: `--semp-url` (queues cmd, default `http://localhost:8080`), `--smf-host` (peek cmd, default `tcp://localhost:55554`), `--vpn` (optional for `queues` — omit to discover all; **required** for `peek`, since an SMF connection is always scoped to exactly one VPN), `--username`, `--password`.

## Auth

Basic auth against SEMP (management plane) and against the SMF connection used for peek. mTLS is supported by both the SEMP API and the messaging client but not wired up here — add if your broker requires it.

## Next step

Decide how this CLI becomes a real service, same as the Kafka adapter — wire into the ingestion/poller described in [`/docs/architecture.md §4`](../../docs/architecture.md#4-architecture). IBM MQ (Phase 3) is next per the build order.
