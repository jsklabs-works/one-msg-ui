# one-msg-ui

A single UI to ease day-to-day operations and support across heterogeneous messaging systems — **IBM MQ**, **Apache Kafka**, **Solace PubSub+**, **RabbitMQ**, and **ActiveMQ Artemis**, with more on the way. Read-only: monitoring (queue depth, consumer lag, health) plus non-destructive message browsing — not an admin console, and deliberately not an alerting tool (see Scope below).

Each system is fundamentally different (point-to-point queues vs. distributed log vs. topic/queue hybrid), so rather than faking a one-size-fits-all abstraction, this project normalizes each broker's native metrics into a shared schema and builds the UI against that — see [`docs/architecture.md`](docs/architecture.md) for the full design.

## Scope

- **In v1:** cross-broker monitoring (depth/lag/health) + non-destructive message browsing (peek at what's on a queue/topic), across prod/uat/dev from day one.
- **Out of scope, permanently:** alerting/paging — this sits alongside each broker's native alerting tools, never replaces or owns alert rules.
- **Deferred:** admin actions (create/delete/purge/ACLs), produce/consume/replay of messages.
- Full rationale and the resolved open questions (credential storage, environment scope, dashboard vs. drill-down usage) are in [`docs/architecture.md §7`](docs/architecture.md#7-decisions-resolved-2026-09-12).

## Status

- **Kafka adapter** — Phase 1 complete: topic/consumer-group discovery, lag calculation, connectivity + replication (ISR/under-replicated/offline) health, and non-destructive message peek, tested against a local Docker Kafka. See [`adapters/kafka/README.md`](adapters/kafka/README.md) to run it.
- **Solace adapter** — Phase 2 complete: VPN/queue monitoring (SEMP v2), connectivity + spool-usage health, and non-destructive message browsing (via the official `solace-pubsubplus` client), verified against a real broker. See [`adapters/solace/README.md`](adapters/solace/README.md) to run it.
- **IBM MQ adapter** — Phase 3 complete: queue monitoring (MQSC-over-REST), connectivity + capacity health, and non-destructive message peek (REST Messaging API), verified against a real queue manager. No PCF, no native client. See [`adapters/mq/README.md`](adapters/mq/README.md) to run it.
- **RabbitMQ adapter** — Phase 5 complete: vhost/queue monitoring, node connectivity + capacity health, and non-destructive message peek — all through one API (the management HTTP API), no AMQP client needed at all. Verified against a real broker (Docker). See [`adapters/rabbitmq/README.md`](adapters/rabbitmq/README.md) to run it.
- **ActiveMQ Artemis adapter** — Phase 5 complete: address/queue monitoring, broker connectivity + capacity health, and non-destructive message peek — all through Jolokia (JMX-over-HTTP, bundled with the broker's own web console), no JMS/core client needed. Verified against a real broker (Docker). See [`adapters/activemq/README.md`](adapters/activemq/README.md) to run it.
- **API layer** — MVP working: a FastAPI service fanning out to all five adapters, serving the unified model as JSON, with graceful per-broker degradation. Live passthrough, not yet the poller+store pipeline from architecture.md §4 — see [`api/README.md`](api/README.md).
- **UI** — MVP working: React + Vite + TypeScript, tabbed (a Dashboard tab plus one real-route tab per connected broker, so multiple brokers can be inspected simultaneously in separate browser tabs), with per-resource drill-down and message peek. Built and verified end-to-end in a real browser against real data from all five systems. See [`ui/README.md`](ui/README.md).

## Layout

```
adapters/     one module per broker type, each translating that broker's native
              monitoring API into the shared data model (see docs/architecture.md §3-4)
  kafka/
  mq/
  solace/
  rabbitmq/
  activemq/
api/          unified API layer over the normalized model, consumed by the UI
ui/           the dashboard itself
docs/         architecture and design docs
```

## Build order

Per the architecture doc's phased rollout: **Kafka first** (Phase 1), then Solace (Phase 2), then IBM MQ (Phase 3), then RabbitMQ and ActiveMQ Artemis (Phase 5 — Phase 4 was the deferred admin/produce/replay scope, still not built). Kafka is the hardest case to get the abstraction right for (no native "queue depth" concept), so validating the shared schema against it first de-risks the rest. Azure Service Bus and SQS/SNS are next, once there's an emulator/cloud-namespace verification story to match the Docker-broker pattern every adapter so far has used.

## Getting started

[`docker-compose.yml`](docker-compose.yml) runs four of the five brokers locally (everything except Solace — see below):

| Service | `docker compose up -d <service>` | Ports | Default credentials |
|---|---|---|---|
| Kafka | `kafka` | `9092` (broker) | none (PLAINTEXT) |
| IBM MQ | `mq` | `1414` (MQI), `9543` (web console + REST) | admin/adminpassw0rd, app/apppassw0rd |
| RabbitMQ | `rabbitmq` | `5673` (AMQP, unused by the adapter), `15672` (management API + console) | admin/admin |
| ActiveMQ Artemis | `activemq` | `61616` (CORE/AMQP/MQTT/OPENWIRE/STOMP, unused by the adapter), `8161` (web console + Jolokia) | admin/admin |

**Kafka** (self-contained — spins up its own local broker):

```bash
docker compose up -d          # local single-node Kafka (KRaft, no ZooKeeper)
cd adapters/kafka
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m kafka_adapter.main topics
```

**Solace** (needs a broker you already have, or a local Solace PubSub+ Docker container — not bundled in this repo's docker-compose):

```bash
cd adapters/solace
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m solace_adapter.main queues
```

**IBM MQ** (self-contained — spins up its own local queue manager; amd64-only image, emulated on Apple Silicon so startup is slower):

```bash
docker compose up -d mq       # local queue manager (QM1)
cd adapters/mq
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m mq_adapter.main queues --pattern "DEV.*"
```

**RabbitMQ** (self-contained — spins up its own local broker):

```bash
docker compose up -d rabbitmq   # local broker, management API on :15672
cd adapters/rabbitmq
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m rabbitmq_adapter.main queues
```

**ActiveMQ Artemis** (self-contained — spins up its own local broker):

```bash
docker compose up -d activemq   # local broker, web console + Jolokia on :8161
cd adapters/activemq
python3 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python scripts/seed.py                    # optional: sample data
./.venv/bin/python -m activemq_adapter.main queues
```

See [`adapters/kafka/README.md`](adapters/kafka/README.md), [`adapters/solace/README.md`](adapters/solace/README.md), [`adapters/mq/README.md`](adapters/mq/README.md), [`adapters/rabbitmq/README.md`](adapters/rabbitmq/README.md), and [`adapters/activemq/README.md`](adapters/activemq/README.md) for the full picture — message peek/browsing, connection flags, and running each test suite.

**Full stack, once the brokers above are up:**

```bash
cd api
python3 -m venv .venv
./.venv/bin/pip install -e ../adapters/kafka -e ../adapters/solace -e ../adapters/mq -e ../adapters/rabbitmq -e ../adapters/activemq -e ".[dev]"
./.venv/bin/uvicorn api.main:app --port 8010          # in one terminal

cd ui
npm install
npm run dev                                            # in another; opens on :5173
```

See [`api/README.md`](api/README.md) and [`ui/README.md`](ui/README.md) for endpoint details, broker configuration, and what's verified vs. not.

## Packaging for distribution

Everything above is the *dev* setup — the UI on its own Vite dev server, hitting the API over CORS. For handing this to someone else to actually run, the API can serve the UI's own built static files from the same process (see `main.py`'s `ONE_MSG_UI_STATIC_DIR` block) — one process, one port, no separate frontend server. Two ways to get there, both verified live:

### Docker (no Python/Node toolchain needed on the machine that runs it)

**Pre-built image** (after a release has been published — see below, no local build needed):

```bash
docker run -d -p 8010:8010 -v $(pwd)/data:/data ghcr.io/jsklabs-works/one-msg-ui:latest
```

**Build it yourself** (works right now, no release required):

```bash
docker build -t one-msg-ui .
docker run -d -p 8010:8010 -v $(pwd)/data:/data one-msg-ui
```

[`Dockerfile`](Dockerfile) is a two-stage build: `npm run build` compiles the UI (with `VITE_API_BASE_URL=""`, so its requests go to the same origin serving it, not a hardcoded `localhost:8010`), then a Python stage installs the API and all five adapter packages from source and copies the built UI in — everything the "Full stack" section above installs by hand, done once at image-build time. The image never bakes in `config/brokers.json`'s real connection details; mounting `-v $(pwd)/data:/data` persists whatever brokers you add through the UI itself across restarts (an unmounted `/data` just means "start with zero brokers configured, same as a fresh checkout" — writes still succeed, they just don't survive removing the container). Point it at brokers running on your own machine using `host.docker.internal` instead of `localhost` in each broker's connection field (Docker's own DNS name for reaching the host from inside a container — this is what actually differs from the dev setup above, not anything this project does).

Verified live: built the image, ran it standalone (empty-state landing page, all five system types listed), added a real RabbitMQ and a real ActiveMQ broker running on the host via `host.docker.internal`, confirmed resources/health loaded through the containerized app, and confirmed a broker added with `/data` mounted was still there after `docker restart`.

**HTTPS by default.** [`docker-entrypoint.sh`](docker-entrypoint.sh) generates a self-signed certificate (`openssl req -x509`, already present in the base image) before starting uvicorn with `--ssl-keyfile`/`--ssl-certfile` — visit **`https://localhost:8010`**, not `http://`. This is deliberate, not a placeholder: every user runs this on their own machine, so there's no shared public domain a real CA could issue a trusted cert for — self-signed is what "HTTPS with no public domain" actually looks like, the same as any local HTTPS dev setup. Your browser will show a one-time "not secure" warning (the cert isn't from a recognized CA) — click through it once. Mounting `-v $(pwd)/data:/data` also persists the generated certificate at `/data/tls/`, so restarting the container reuses the same cert instead of generating a new one (and a new browser warning) every time — confirmed live: same certificate fingerprint before and after a `docker restart`. Override the cert location with `ONE_MSG_UI_TLS_DIR` if you'd rather point it somewhere other than `/data/tls`, or drop in your own real certificate at that path (as `cert.pem`/`key.pem`) if you do have one — the entrypoint only generates one when neither file already exists.

### Plain build (already have Python 3.10+ and Node on the target machine)

```bash
# UI — produces ui/dist, servable as static files by anything, including the API itself
cd ui && VITE_API_BASE_URL="" npm run build

# API + adapters — same installs as the Dockerfile, just not inside a container
cd ../api
python3 -m venv .venv
./.venv/bin/pip install ../adapters/kafka ../adapters/solace ../adapters/mq ../adapters/rabbitmq ../adapters/activemq .

# Serve both from one process. ONE_MSG_UI_STATIC_DIR is REQUIRED here — its
# fallback default only resolves correctly for an *editable* dev install
# (`pip install -e .`), where the installed package's __file__ still points
# at this source tree. The plain install above is not editable (deliberately
# — same as the Dockerfile's), so that fallback silently can't find ui/dist:
# confirmed live, a fresh non-editable install without this env var served a
# plain 404 at "/" instead of the UI.
ONE_MSG_UI_STATIC_DIR=$(pwd)/../ui/dist \
ONE_MSG_UI_BROKERS_CONFIG=/path/to/your/brokers.json \
  ./.venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 8010
```

To hand off *just* the Python side without copying source (e.g. installing on a server that never sees this git checkout), build real wheels instead of installing from local paths: `python -m build` inside each `adapters/*` directory and inside `api/` (each already has the `[build-system]` a wheel needs) produces a `.whl` under that package's own `dist/` — copy those five wheels plus `ui/dist` to the target machine, `pip install *.whl` there, and still set `ONE_MSG_UI_STATIC_DIR` to wherever `ui/dist` ended up (same reason as above — it's not derivable from the installed wheel's own location).

For HTTPS here too, generate the same kind of self-signed certificate the Docker image creates automatically (`openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 825 -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"`) and add `--ssl-keyfile key.pem --ssl-certfile cert.pem` to the `uvicorn` command above.

### Releasing a new version

[`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml) builds the same `Dockerfile` above and pushes it to `ghcr.io/jsklabs-works/one-msg-ui` — the pre-built-image command earlier on this page. It runs on a version tag, not on every push to main, so `latest` only moves when someone actually cuts a release:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Pushing the tag also publishes `v0.1.0` and `v0.1` images alongside `latest`, so a deployment can pin to an exact version instead of always tracking `latest`. To re-publish without a new tag (e.g. after a registry hiccup), run the workflow manually from the repo's Actions tab (`workflow_dispatch`) — that run only updates `latest`, since there's no version tag to derive `v0.1.0`-style tags from.

Same distinction as the Docker path: point brokers.json's connection fields at wherever your real brokers actually are — `localhost` only works if the broker and this process are on the same machine.
