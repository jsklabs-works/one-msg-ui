# One UI for Messaging — Unified Monitoring Architecture

**Scope:** A single monitoring console covering IBM MQ, Apache Kafka, and Solace PubSub+, showing queue/topic depth, consumer lag, and broker/system health.
**Not in scope (v1):** admin actions (create/delete queues, ACL changes), message browsing/replay, produce/consume of test messages. These are natural v2 candidates once the monitoring layer proves out — see Phase 4.

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
| **IBM MQ** | REST Admin API (`/ibmmq/rest/v1/admin/qmgr/{qmgr}/...`); PCF (Programmatic Command Format) for anything not yet in REST | JSON (REST) / binary MQI messages (PCF) | Basic auth, or bearer/OAuth via Cloud Pak; mTLS for transport | Queue depth (`CURDEPTH`), max depth, channel status, queue manager status, connection counts |
| **Apache Kafka** | Kafka Admin API (offsets, group metadata) + JMX (broker/client internals); `kafka-consumer-groups` CLI wraps the Admin API | Binary protocol / JMX MBeans; CLI output is text/JSON | SASL/SCRAM, mTLS, or Kerberos depending on cluster config | Log-end-offset vs. committed-offset per partition (→ lag), broker ISR/under-replicated-partition counts, request/response latency |
| **Solace PubSub+** | SEMP v2 **monitor** API (`GET /SEMP/v2/monitor/...`) | JSON (REST) | Basic auth or mTLS against the broker's management plane | Per-queue spooled messages/bytes, discards, redelivery counts; per-VPN throughput and spool usage; broker-level health, memory, spool usage, uptime |

Notable gotchas worth designing around:

- **Kafka lag is not always reliable.** Consumers using manual `assign()` rather than group `subscribe()` don't report lag through the standard group-metadata path; groups that go `EMPTY` for over a day stop emitting lag metrics entirely. The adapter needs a fallback (compute lag directly from log-end-offset vs. last-committed-offset via the Admin API) rather than trusting a single source.
- **IBM MQ's REST API doesn't cover 100% of PCF's surface.** Some queue/channel attributes are REST-only in recent versions, others still require PCF. The adapter should be built REST-first with a PCF fallback path, not PCF-first.
- **Solace scoping is two-level** (VPN → queue/topic), so the normalized model needs a VPN/namespace concept that MQ and Kafka don't have — don't flatten it away, or multi-tenant Solace brokers become unreadable in the UI.

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
```

Design rationale: rather than inventing a single "queue depth" field that's forced onto Kafka (which has no such number), the model keeps **depth** and **consumer_lag** as separate fields that are simply null/not-applicable for systems where the concept doesn't exist. The UI layer decides what to show per system type instead of the data model lying about equivalence. This avoids the classic mistake of these unification efforts — presenting a fake apples-to-apples number that misleads an on-call engineer at 3am.

---

## 4. Architecture

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  MQ Adapter │   │Kafka Adapter│   │Solace Adapter│
│ (REST+PCF)  │   │(AdminClient │   │  (SEMP v2)   │
│             │   │  + JMX)     │   │              │
└──────┬──────┘   └──────┬──────┘   └──────┬───────┘
       │ normalize to common schema         │
       └───────────────┬─────────────────────┘
                        ▼
            ┌───────────────────────┐
            │  Ingestion / Poller   │  (scheduled pulls; push where
            │  service               │   the broker supports webhooks/
            └───────────┬───────────┘   streaming metrics)
                        ▼
            ┌───────────────────────┐
            │  Time-series store    │  (Prometheus, or Timescale/
            │  + metadata store      │   Influx — see §5)
            └───────────┬───────────┘
                        ▼
            ┌───────────────────────┐
            │  Unified API layer    │  (GraphQL/REST over the
            │                        │   normalized model)
            └───────────┬───────────┘
                        ▼
            ┌───────────────────────┐
            │        UI              │  cross-system dashboard,
            │                        │  per-broker drill-down,
            │                        │  alerting rules
            └───────────────────────┘
```

Each adapter is an independent, replaceable module — this is the load-bearing design decision. Adding RabbitMQ or Azure Service Bus later means writing one more adapter against the existing schema, not touching the UI or the four other layers. Each adapter owns translating its system's native vocabulary (CURDEPTH, log-end-offset, spooled messages) into the common `Resource`/`ConsumerGroup`/`HealthEvent` shapes above, and nothing downstream needs to know which broker a metric came from beyond the `system_type` tag.

Polling vs. push: all three systems are comfortably poll-based for monitoring purposes (15–60s intervals are standard for capacity/lag dashboards). None require you to run a push-based pipeline for v1 — that's added complexity for marginal freshness gain at this stage.

---

## 5. Suggested tech stack

Kept ordinary and boring on purpose — this is not the part of the system worth taking risk on:

- **Adapters:** small services (one per broker type), written in whatever language your team already runs in production — Kafka's ecosystem favors Java/Kotlin or Python (`kafka-python`/`confluent-kafka`) for the Admin client; MQ and Solace both have first-class REST clients so any language works there.
- **Metrics storage:** Prometheus is the pragmatic default — community exporters already exist for Solace (`solace-prometheus-exporter`) and for Kafka (`kafka-exporter`, JMX exporter), so two of three adapters can start as "run the existing exporter + a thin normalization shim" rather than from scratch. IBM MQ has community and IBM-supported Prometheus exporters too.
- **Metadata / config store:** small relational DB (broker inventory, thresholds, alert rules) — Postgres is fine.
- **API layer:** REST or GraphQL over the normalized model; GraphQL is worth it if the UI needs to query "all resources across all brokers above 80% depth" type cross-cutting views often.
- **UI:** whatever your team's standard web stack is — this is a fairly conventional dashboard/table/drill-down app, nothing about the messaging domain constrains that choice.

---

## 6. Phased rollout

1. **Phase 1 — Kafka only.** Best-documented Admin API, richest existing tooling (exporters, `kafka-consumer-groups` CLI to validate against), and consumer lag is the metric most teams care about most urgently. Proves the adapter pattern and the normalized schema end-to-end with one system.
2. **Phase 2 — Add Solace.** REST-based (SEMP v2), closest in shape to MQ, existing community Prometheus exporter to lean on. Validates the VPN/namespace concept in the data model.
3. **Phase 3 — Add IBM MQ.** REST Admin API first; identify which specific attributes your environment needs that are PCF-only, and add a PCF fallback path only for those rather than building full PCF support speculatively.
4. **Phase 4 — Expand scope**, informed by what Phase 1–3 actually needed: message browsing/inspection, admin actions, produce/consume test tooling, additional broker types (RabbitMQ, ActiveMQ, Azure Service Bus, SQS/SNS).

Kafka-first is a deliberate choice against your original "IBM MQ, Kafka, Solace" ordering — it's the one where getting the abstraction right matters most (since it's the most different from the other two), so it's worth validating the model against the hardest case before the easier ones.

---

## 7. Open questions to resolve before build

- **Alerting ownership:** does this UI *replace* existing broker-specific alerting (IBM's own tooling, Confluent Control Center, Solace's native monitor), or sit alongside it as a read-only cross-system view? This affects whether alert-rule state lives here or is just visualized from elsewhere.
- **Credential management:** each adapter needs broker credentials (MQ basic auth/mTLS, Kafka SASL/mTLS, Solace basic auth/mTLS) — where do these live (vault, k8s secrets) and who rotates them?
- **Multi-environment scope:** is this one UI across prod/uat/dev for all three systems, or prod-only initially? Affects the `environment` field's importance and access-control design from day one.
- **On-call usage pattern:** is the primary use case a dashboard someone glances at, or something an on-call engineer drills into during an incident? This should drive whether Phase 1 prioritizes broad coverage (many brokers, shallow detail) or deep detail on fewer brokers.

---

*Sources consulted: [IBM MQ REST Admin API overview](https://www.mainframemaster.com/tutorials/mq/rest-admin-apis) · [Solace Prometheus Exporter (SEMP v2 mapping)](https://github.com/solacecommunity/solace-prometheus-exporter) · [Confluent: Monitor Kafka Consumer Lag](https://docs.confluent.io/cloud/current/monitoring/monitor-lag.html)*
