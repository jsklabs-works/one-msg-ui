# Solace PubSub+ adapter

**Status:** not started (Phase 2 — build after Kafka)

## Responsibility

Translate Solace's native monitoring surface into the shared model defined in [`/docs/architecture.md`](../../docs/architecture.md#3-unified-data-model):

- `Resource` — one per queue (spooled messages/bytes → `depth_current`; configured spool quota → `depth_max`) or topic
- Solace is scoped two levels deep (**Message VPN → queue/topic**) — keep the VPN/namespace concept explicit in this adapter's output rather than flattening it away, or multi-tenant brokers become unreadable in the UI
- `HealthEvent` — broker health, memory, spool usage, uptime

## Data sources

- **SEMP v2 monitor API** (`GET /SEMP/v2/monitor/...`) — JSON over REST, this is the only source needed
- The community [`solace-prometheus-exporter`](https://github.com/solacecommunity/solace-prometheus-exporter) already maps most of what this adapter needs (`QueueStats`, `QueueDetails`, `QueueRates`, `Vpn`, `VpnStats`, `VpnSpool`, `Health`, `Memory`, `Spool`) — worth running as-is and writing a thin normalization shim on top rather than reimplementing the SEMP calls from scratch

## Auth

Basic auth or mTLS against the broker's management plane.

## Next step

Stand up the community Prometheus exporter against a test broker, confirm it surfaces what's needed per-VPN, then write the shim that maps its output into the shared `Resource` schema.
