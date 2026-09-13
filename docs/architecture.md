# One UI for Messaging — Unified Monitoring Architecture

**Scope:** A single console covering IBM MQ, Apache Kafka, and Solace PubSub+, showing queue/topic depth, consumer lag, broker/system health, **and read-only message browsing/inspection** (peek at what's sitting on a queue/topic without consuming it) — the goal is to ease day-to-day messaging operations and support, not just provide a monitoring view.
**Not in scope (v1):** admin actions (create/delete queues, resize, ACL changes), produce/consume or replay of messages, and alerting/paging. Alerting is a deliberately separate concern — this tool is a read-only operational/support view that sits alongside each broker's native alerting (IBM's tooling, Confluent Control Center, Solace's native monitor), not a replacement for it; alert-rule ownership is not on this project's roadmap. Admin actions and produce/consume are natural v2 candidates once monitoring + browsing prove out — see Phase 4.

---

## 1. Why this is non-trivial

IBM MQ, Kafka, and Solace are architecturally different messaging paradigms, not just different vendors of the same thing:

- **IBM MQ** — point-to-point queue manager. "Depth" is a literal count of messages sitting on a queue. Monitoring is object-status-based (queue, channel, queue manager).
- **Kafka** — distributed commit log. There's no "depth" in the MQ sense; the closest analogue is **consumer lag** — the gap between the latest offset written to a partition and the offset a consumer group has committed. Health is a cluster-wide, partition-distributed concept (broker/ISR/replication status), not a per-queue one.
- **Solace PubSub+** — event broker supporting both queues (guaranteed, point-to-point/durable) and topics (pub/sub). It has queue depth like MQ *and* topic-based delivery like Kafka, scoped per **Message VPN**.

Because "queue depth," "consumer lag," and "health" mean structurally different things per system, a unified UI can't just proxy each system's native API and slap a shared theme on top — it needs a **normalized data model** that each system's data is mapped into, with the UI built against that model rather than against any one broker's vocabulary.

---

## 2. Native monitoring surfaces per system

| System | Primary monitoring API | Format | Auth | Key metrics exposed |
|---|---|---|---|---|
| **IBM MQ** | REST Admin API, but not its plain resource endpoints (those are config-only — `curdepth`/`maxdepth` are rejected there). Runtime status instead comes via **MQSC-over-REST** (`POST /ibmmq/rest/v1/admin/action/qmgr/{qmgr}/mqsc`, running a real MQSC command and returning its console text output). No PCF or native client needed — confirmed in Phase 3, see adapter note below. | JSON request, free-text MQSC console output in the response (needs parsing — see `mqsc.py`) | Basic auth, or bearer/OAuth via Cloud Pak; mTLS for transport | Queue depth (`CURDEPTH`), max depth, queue manager status, open input/output handle counts |
| **Apache Kafka** | Kafka Admin API (offsets, group metadata, **and** per-partition replica/ISR/offline-replica state via topic metadata — ISR/under-replication does *not* need JMX, see adapter note below); JMX only for broker-internal metrics the Metadata API doesn't carry (request latency percentiles, handler idle ratio); `kafka-consumer-groups` CLI wraps the Admin API | Binary protocol / JMX MBeans; CLI output is text/JSON | SASL/SCRAM, mTLS, or Kerberos depending on cluster config | Log-end-offset vs. committed-offset per partition (→ lag), broker ISR/under-replicated-partition counts (Admin API), request/response latency (JMX) |
| **Solace PubSub+** | SEMP v2 **monitor** API (`GET /SEMP/v2/monitor/...`) | JSON (REST) | Basic auth or mTLS against the broker's management plane | Per-queue spooled messages/bytes, discards, redelivery counts; per-VPN throughput and spool usage; broker-level health, memory, spool usage, uptime |

Notable gotchas worth designing around:

- **Kafka lag is not always reliable.** Consumers using manual `assign()` rather than group `subscribe()` don't report lag through the standard group-metadata path; groups that go `EMPTY` for over a day stop emitting lag metrics entirely. The adapter needs a fallback (compute lag directly from log-end-offset vs. last-committed-offset via the Admin API) rather than trusting a single source.
- **IBM MQ's plain REST resource endpoints are config-only, not runtime status.** Confirmed in Phase 3: `GET .../queue/{name}?attributes=curdepth` is rejected outright — no PCF fallback was actually needed, though; MQSC-over-REST (see table row above and [`adapters/mq/README.md`](../adapters/mq/README.md)) covered every runtime attribute this project needed.
- **Solace scoping is two-level** (VPN → queue/topic), so the normalized model needs a VPN/namespace concept that MQ and Kafka don't have — don't flatten it away, or multi-tenant Solace brokers become unreadable in the UI. Learned the hard way even after building the concept in: the first adapter/UI pass still hardcoded one VPN per broker connection, so a VPN created on the broker after the connection was added silently never appeared — fixed by making `vpn_name` an optional *filter* (unset = discover every VPN those credentials can see) rather than a required scope. See [`adapters/solace/README.md`](../adapters/solace/README.md).

**Auth, as actually implemented (not just documented) as of the API layer's add-broker flow:** Kafka supports PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL with SASL PLAIN credentials; Solace and MQ both support a TLS-certificate-verification toggle (on by default for Solace, off by default for MQ to match its self-signed dev cert). Not implemented: mTLS client certificates anywhere, Kerberos for Kafka, OAuth/Cloud Pak for MQ. See [`api/README.md`'s TLS/SASL section](../api/README.md#tlssasl) for exactly what's wired through where.

---

## 3. Unified data model

Everything each adapter emits gets normalized into a small set of common shapes before it reaches the UI or storage layer:

```
Broker
  id, system_type (mq | kafka | solace), name, environment (prod/uat/dev), status (up/down/degraded)

Resource                      # the "queue-ish" thing — maps MQ queue, Kafka topic+partition-set, Solace queue
  id, broker_id, namespace (qmgr | vpn | cluster), name, kind (queue | topic)
  depth_current               # MQ: CURDEPTH · Solace: spooled messages · Kafka: sum of partition lag (see below)
  depth_max                    # MQ: MAXDEPTH · Solace: configured spool quota · Kafka: n/a (log-based, no ceiling)
  consumer_lag                 # Kafka: native concept · MQ/Solace: derived as depth (no per-consumer offset model)
  throughput_in, throughput_out
  last_updated

ConsumerGroup                  # Kafka-specific, modeled explicitly rather than forced into Resource
  id, broker_id, group_name, state, member_count
  partitions[]: { topic, partition, log_end_offset, committed_offset, lag }

HealthEvent
  broker_id, severity (ok | warn | critical), category (connectivity | capacity | replication | spool), message, timestamp

MessageSample                  # non-destructive peek, not a consume — see §3.1
  resource_id, message_id (or offset/partition for Kafka), timestamp, topic, headers, body_preview, size_bytes
  # topic: the actual destination the message arrived on. Kafka: always the
  # topic being browsed. Solace: can genuinely differ from the queue being
  # browsed — a message published to a topic and spooled onto the queue via
  # subscription reports that topic, not the queue name (uses the official
  # client's get_destination_name(), verified live). MQ: always null, no
  # topic concept. headers is the message's own properties (Kafka record
  # headers, Solace get_properties(), MQ's ibm-mq-md-* response headers) —
  # this was being fetched by the API layer but silently dropped by the UI
  # until this was added; a support engineer needs these, not just the body.
```

Design rationale: rather than inventing a single "queue depth" field that's forced onto Kafka (which has no such number), the model keeps **depth** and **consumer_lag** as separate fields that are simply null/not-applicable for systems where the concept doesn't exist. The UI layer decides what to show per system type instead of the data model lying about equivalence. This avoids the classic mistake of these unification efforts — presenting a fake apples-to-apples number that misleads an on-call engineer at 3am.

### 3.1 Message browsing/peek (in scope for v1)

A non-destructive "what's actually on this queue?" view — the most common support-ticket need — without consuming or altering anything. Per-system mechanics differ enough to call out explicitly:

- **IBM MQ** — confirmed in Phase 3: the REST *Messaging* API's plain `GET .../queue/{q}/message` is non-destructive by default — verified with 2 distinct messages and repeated GETs, depth never changed. No special "browse" parameter needed or available. Real limitation: no browse cursor — it always returns the same head-of-queue (oldest) message, so this adapter can only ever peek one message per queue, not the next N like Kafka/Solace. A true multi-message browse needs `MQGMO_BROWSE_NEXT` via MQI/pymqi, deliberately not used to avoid that native-dependency risk.
- **Kafka** — a consumer with a scratch/no-commit group (or `assign()` + manual `seek()`), poll without committing offsets. Non-destructive as long as no group-commit happens; the adapter must guarantee it never commits on the browse path.
- **Solace** — confirmed in Phase 2 (see [`adapters/solace/README.md`](../adapters/solace/README.md)): SEMP v2 *does* have queue message-inspection endpoints (`/queues/{q}/msgs`), but they return metadata only (id, size, timestamp) — never the body. Peeking the actual payload needs a real client-protocol connection; the official `solace-pubsubplus` Python client's `MessageQueueBrowser` provides a proper broker-native non-destructive browse (verified: spooled count unchanged after browsing). This is a second connection type (SMF) alongside the SEMP-based monitoring connection — extra adapter complexity as flagged, but a well-supported path, not a workaround.

Message bodies may contain sensitive payload data — `body_preview` should be capped/truncated by default and access to full-body view should go through the same access control as everything else (see §7 environment/access-control decisions).

---

## 4. Architecture

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  MQ Adapter  │  │ Kafka Adapter│  │Solace Adapter│
│(MQSC-over-   │  │(AdminClient) │  │(SEMP v2+SMF) │
│    REST)     │  │              │  │              │
│     done     │  │     done     │  │     done     │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │ normalize to common schema         │
       └───────────────┬─────────────────────┘
                        ▼
            ┌───────────────────────┐
            │  Ingestion / Poller   │  NOT BUILT — MVP's API layer queries
            │  service               │  brokers live on every request
            └───────────┬───────────┘  instead (see api/README.md's
                        ▼              "live passthrough" scope note)
            ┌───────────────────────┐
            │  Time-series store    │  NOT BUILT for the same reason —
            │  + metadata store      │  needed once this handles more than
            └───────────┬───────────┘  a handful of local dev brokers
                        ▼
            ┌───────────────────────┐
            │  Unified API layer    │  done (MVP) — FastAPI, REST,
            │                        │  see api/README.md
            └───────────┬───────────┘
                        ▼
            ┌───────────────────────┐
            │        UI              │  done (MVP) — React, cross-system
            │                        │  overview + per-resource drill-down,
            │                        │  see ui/README.md (no alerting — §7)
            └───────────────────────┘
```

Each adapter is an independent, replaceable module — this is the load-bearing design decision. Adding RabbitMQ or Azure Service Bus later means writing one more adapter against the existing schema, not touching the UI or the four other layers. Each adapter owns translating its system's native vocabulary (CURDEPTH, log-end-offset, spooled messages) into the common `Resource`/`ConsumerGroup`/`HealthEvent` shapes above, and nothing downstream needs to know which broker a metric came from beyond the `system_type` tag.

Polling vs. push: all three systems are comfortably poll-based for monitoring purposes (15–60s intervals are standard for capacity/lag dashboards). None require you to run a push-based pipeline for v1 — that's added complexity for marginal freshness gain at this stage. The MVP API layer approximates this by having the UI poll it every 15s and having it query brokers live rather than actually implementing the poller/store — fine for three local brokers, not the design to keep once this points at real infrastructure.

---

## 5. Suggested tech stack

Kept ordinary and boring on purpose — this is not the part of the system worth taking risk on:

- **Adapters:** small services (one per broker type), written in whatever language your team already runs in production — Kafka's ecosystem favors Java/Kotlin or Python (`kafka-python`/`confluent-kafka`) for the Admin client; MQ and Solace both have first-class REST clients so any language works there. *Implementation note:* all three adapters were built in Python (`kafka-python-ng`; `requests` + the official `solace-pubsubplus` client; `requests` alone for MQ) — fast to iterate, no build step, and none needed a native-dependency workaround (MQ's PCF/pymqi path, assumed necessary going in, turned out unnecessary — see §2's table).
- **Metrics storage:** Prometheus is the pragmatic default — community exporters already exist for Solace (`solace-prometheus-exporter`) and for Kafka (`kafka-exporter`, JMX exporter), so two of three adapters can start as "run the existing exporter + a thin normalization shim" rather than from scratch. IBM MQ has community and IBM-supported Prometheus exporters too.
- **Metadata / config store:** small relational DB (broker inventory, thresholds, encrypted broker credentials) — Postgres is fine. See §7 for the credential-storage decision.
- **API layer:** REST or GraphQL over the normalized model; GraphQL is worth it if the UI needs to query "all resources across all brokers above 80% depth" type cross-cutting views often.
- **UI:** whatever your team's standard web stack is — this is a fairly conventional dashboard/table/drill-down app, nothing about the messaging domain constrains that choice.

---

## 6. Phased rollout

1. **Phase 1 — Kafka only. Done.** Best-documented Admin API, richest existing tooling (exporters, `kafka-consumer-groups` CLI to validate against), and consumer lag is the metric most teams care about most urgently. Adapter pattern and normalized schema proved out end-to-end — see [`adapters/kafka/README.md`](../adapters/kafka/README.md).
2. **Phase 2 — Add Solace. Done.** REST-based (SEMP v2) for monitoring; VPN/namespace concept validated in the data model. Message browsing needed a separate client-protocol connection as anticipated (see §3.1) — the official `solace-pubsubplus` client's `MessageQueueBrowser` covers it well.
3. **Phase 3 — Add IBM MQ. Done.** REST Admin API's plain resource endpoints turned out to be config-only; runtime status (depth, qmgr state) needed MQSC-over-REST instead (see §2). No PCF fallback was needed at all. Message peek uses the REST Messaging API's plain GET, confirmed non-destructive by default — see §3.1 for the real limitation (no browse cursor, one message per queue).
4. **Phase 4 — Expand scope**, informed by what Phase 1–3 actually needed: admin actions, produce/consume test tooling, additional broker types (RabbitMQ, ActiveMQ, Azure Service Bus, SQS/SNS). (Message browsing/inspection moved into v1 scope — see §3.1 and §7.) All three broker adapters (Phase 1-3) are feature-complete; the unified API layer and UI are next.

Kafka-first is a deliberate choice against your original "IBM MQ, Kafka, Solace" ordering — it's the one where getting the abstraction right matters most (since it's the most different from the other two), so it's worth validating the model against the hardest case before the easier ones.

---

## 7. Decisions (resolved 2026-09-12)

- **Alerting ownership:** out of scope, permanently, not just for v1. This is a read-only operations/support view that sits *alongside* each broker's native alerting (IBM's tooling, Confluent Control Center, Solace's native monitor) — it never owns alert-rule state, thresholds-for-paging, or notification routing. The `environment`/`HealthEvent` data this app surfaces is for a human looking at a screen, not for triggering pages.
- **Credential management:** app-owned, stored encrypted in the Postgres metadata store — no external vault/secrets-manager dependency for now, to keep the deployment footprint light (no extra infra to stand up). Encrypt at the column level (e.g. `pgcrypto` or app-level envelope encryption with a single KMS- or env-provided master key) rather than storing plaintext. Revisit if/when this ever touches broker environments with stricter compliance requirements than support tooling typically has.
- **Multi-environment scope:** prod/uat/dev, all in scope from day one. `Broker.environment` is a first-class filter/grouping dimension in the UI and the API from the start, and access control must be designed around it from the start too (e.g. a support engineer scoped to uat/dev shouldn't implicitly get prod visibility) — this is not a "bolt on later" concern.
- **Usage pattern:** both glance-dashboard and incident drill-down, roughly equally weighted. The UI needs two distinct surfaces from early on: a broad cross-broker overview (many brokers/resources, shallow detail — the "what's on fire" view) and a deep per-resource drill-down (single queue/topic, full detail, message browsing per §3.1 — the "let me actually look at this" view). Don't let Phase 1 optimize for only one of these; both are first-class from the Kafka adapter onward.
- **Scope addition — message browsing/inspection is in v1**, not deferred to Phase 4 as originally scoped. This was the standout "operations and support" need: non-destructive peek at queue/topic contents. Admin actions (create/delete/purge/ACLs) and produce/consume/replay remain deferred to Phase 4 — browsing was the specific gap, not a signal to pull all of Phase 4 forward. See §3.1 for per-broker mechanics and the Solace caveat (needs a client-protocol connection, not just SEMP).

---

*Sources consulted: [IBM MQ REST Admin API overview](https://www.mainframemaster.com/tutorials/mq/rest-admin-apis) · [Solace Prometheus Exporter (SEMP v2 mapping)](https://github.com/solacecommunity/solace-prometheus-exporter) · [Confluent: Monitor Kafka Consumer Lag](https://docs.confluent.io/cloud/current/monitoring/monitor-lag.html)*
