# Unified API layer

**Status:** MVP working — a FastAPI service that fans out to all five broker adapters and serves the normalized model (`Broker`, `Resource`, `ConsumerGroup`, `HealthEvent`, `MessageSample`) as JSON, verified end-to-end against the same five real local brokers the adapters were each verified against.

## Responsibility

Serves the normalized model (see [`/docs/architecture.md`](../docs/architecture.md#3-unified-data-model)) to the UI. This is the layer where "kafka has consumer_lag, MQ has depth, Solace has spool usage" actually gets flattened into one shape the UI can render without caring which broker a resource came from — see [`normalize.py`](src/api/normalize.py), which converts each adapter's own dataclasses into the shared pydantic models in [`models.py`](src/api/models.py).

**Important scope note:** this is a **live passthrough**, not the ingestion/poller + time-series store pipeline described in [architecture.md §4](../docs/architecture.md#4-architecture). Every request to `/api/resources` or `/api/health` queries every broker live, synchronously. That's fine for an MVP with a handful of local dev brokers; it is *not* how this should work once there are real brokers and a UI polling every few seconds — that needs the poller-writes-to-a-store design the architecture doc describes, so brokers get polled once on a schedule instead of once per UI request. Treat this API as a proof that the normalization/fan-out logic works, not as the production data path.

## Endpoints

| Method & path | Returns |
|---|---|
| `GET /api/brokers` | All configured brokers, with live `status` (up/down) |
| `GET /api/resources` | All resources across all brokers. Filter with `?environment=`, `?system_type=`, `?broker_id=` |
| `GET /api/resources/{resource_id}` | One resource (404 if not found) |
| `GET /api/health` | All health events. Filter with `?broker_id=` |
| `GET /api/consumer-groups` | Kafka consumer groups (empty for non-Kafka brokers). Filter with `?broker_id=` |
| `GET /api/resources/{resource_id}/messages?limit=N` | Non-destructive peek — architecture.md §3.1. Kafka/Solace/RabbitMQ/ActiveMQ can return up to `limit`; MQ can only ever return the one oldest message (see [`adapters/mq/README.md`](../adapters/mq/README.md)) |
| `POST /api/resources/{resource_id}/messages` `{limit, username, password}` | Same peek, but with credentials for just this call instead of the broker's saved config — a password has no business in a URL query string, hence POST. For when the broker's saved credentials don't work for this specific resource (see next section) |
| `GET /api/system-types` | The broker types this instance can connect to (today: Kafka, Solace, MQ, RabbitMQ, ActiveMQ) plus each one's real connection `fields` — drives the UI's "add broker" form dynamically, see [`config.py`](src/api/config.py)'s `FIELD_SPECS` |
| `POST /api/brokers` | Add a broker: `{type, name, environment, config}`. Rejects a duplicate display name or a second connection to the same physical broker with 409 (see below), then actually tries to connect before saving anything (422 with the real error if it fails) — see [`aggregator.py`](src/api/aggregator.py)'s `test_connection()` |
| `POST /api/brokers/import` | Bulk add: `{brokers: [{type, name, environment, config}, ...]}` — the same shape as [`config/brokers.json`](config/brokers.json) itself, so that file can be uploaded as-is. Runs every check `POST /api/brokers` does, per entry, but never fails the whole request on one bad entry — always 200, with one `{name, type, status, detail, broker}` result per entry (`status` one of `added`/`duplicate_name`/`duplicate_connection`/`invalid`/`connection_failed`) — see [`main.py`](src/api/main.py)'s `_add_one_broker()` |
| `GET /api/brokers/export` | The other half of the round trip: `{brokers: [{id, type, name, environment, config}, ...]}` — every configured broker, full connection config (credentials included) — in exactly `config/brokers.json`'s own shape, so it can be fed straight back into `POST /api/brokers/import` or saved as another instance's `config/brokers.json` unchanged |
| `DELETE /api/brokers/{broker_id}` | Forget a broker (just the connection entry — never touches the broker itself) |

A broker that's unreachable is reported with `status: "down"` and a `critical`/`connectivity` `HealthEvent` rather than failing the whole request — see [`aggregator.py`](src/api/aggregator.py)'s `fetch_all()`. Four-out-of-five brokers responding is more useful to an on-call engineer than a blank screen.

## Broker configuration

[`config/brokers.json`](config/brokers.json) lists the brokers this instance talks to and their connection details, loaded by [`config.py`](src/api/config.py) — and now also *written* by it, via `POST /api/brokers`/`DELETE /api/brokers/{id}` (see [`save_broker_configs`](src/api/config.py)). Per [architecture.md §7](../docs/architecture.md#7-decisions-resolved-2026-09-12), credentials belong in an encrypted Postgres store long-term — this plain JSON file is an explicit MVP placeholder, not a decision to keep committing plaintext credentials. The checked-in file only has the well-known local dev defaults already documented in each adapter's README (not real secrets); replace it (or point `ONE_MSG_UI_BROKERS_CONFIG` at a different path) before this ever talks to anything real. A missing config file is treated as zero brokers configured, not an error — that's the UI's empty-state landing scenario.

### TLS/SASL

Each `FIELD_SPECS` entry includes the real security options each adapter now actually supports (not just cosmetic form fields):

- **Kafka** — `use_tls` (SSL), `sasl_username`/`sasl_password` (SASL PLAIN), `ssl_cafile` (custom CA). Combined into the right `security_protocol` (PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL) by [`aggregator._kafka_security_kwargs`](src/api/aggregator.py), which calls straight through to `kafka_adapter.client.security_kwargs` — verified against `KafkaConsumer.DEFAULT_CONFIG`'s real keys, not guessed.
- **Solace** — `verify_certificate` (default on; uncheck for a broker with a self-signed cert) + `ssl_cafile`. Wired to the SEMP client's `requests.Session.verify` and, for message peek, a real `solace.messaging.config.transport_security_strategy.TLS` strategy on the SMF connection.
- **MQ** — `verify_tls` (default off, matching the dev queue manager's self-signed cert) + `ssl_cafile`. `MQAdapter` already accepted this; it just wasn't exposed or configurable from here before.
- **RabbitMQ** — `verify_certificate` (default on; uncheck for a broker with a self-signed cert) + `ssl_cafile`, same convention as Solace's fields of the same names — one credential set covers both monitoring and peek here, so there's no separate app/admin split like MQ's.
- **ActiveMQ** — `verify_certificate` (default on) + `ssl_cafile`, same convention as Solace/RabbitMQ — one credential set (basic auth against Jolokia) covers both monitoring and peek.

**`ssl_cafile` — a custom CA bundle, not just verify-or-don't:** found by inspection, not a bug report — every adapter's `verify_certificate`/`verify_tls` param already worked with either a bool or a CA-bundle path at runtime (`requests.Session.verify` accepts both), but only Kafka's form ever offered a field for the path. Anyone running Solace/MQ/RabbitMQ/ActiveMQ behind a real private/corporate CA (not self-signed, not a public CA) had no option but to fully disable verification just to connect. [`config.resolve_tls_verify`](src/api/config.py) is the one place this logic lives: a configured `ssl_cafile` always wins over the plain checkbox (a CA path already implies "yes, verify, against this bundle"); the checkbox is the fallback when no path is set. Used by every fetcher and every peek-credential path in [`aggregator.py`](src/api/aggregator.py) — the same helper, not five copies of the same `if`.

Not covered: mTLS (client certificates), Kerberos, and SASL mechanisms beyond PLAIN. Add `FIELD_SPECS` entries and the matching adapter support if a real broker needs those.

### Duplicate-connection detection

Adding a broker checks two different things, both before anything is persisted or connected to:

1. **Duplicate display name** — a second broker named the same as an existing one (409, `"a broker named '...' already exists"`).
2. **Duplicate connection** — a second broker pointed at the *same physical endpoint* as an existing one, whatever it's named — e.g. two Solace entries with the same SEMP URL, two Kafka entries with the same `bootstrap_servers`, two RabbitMQ entries with the same management API URL, two ActiveMQ entries with the same console URL, or two MQ entries with the same admin URL *and* queue manager name (409, `"... broker '...' already points at this same connection"`).

The identity check ([`config.connection_identity`](src/api/config.py), used by [`aggregator.BrokerRegistry.find_duplicate`](src/api/aggregator.py)) is deliberately narrower than "identical config": different credentials or a different Solace VPN filter on the same SEMP URL is still a legitimate second connection (that's the whole point of the per-resource peek credentials below), and two MQ queue managers on the same host are two different brokers. Only the fields that actually identify *which broker* a connection reaches are compared, normalized for case/whitespace/trailing slash so `http://localhost:8080` and `HTTP://Localhost:8080/` are correctly seen as the same broker.

### Per-resource peek credentials

One broker config stores one set of credentials, but that's not always enough for peek specifically: Solace is the concrete case — SEMP admin credentials are broker-wide (that's what makes [multi-VPN discovery](../adapters/solace/README.md) work at all), but an SMF connection for peek authenticates *per VPN*, and a VPN can genuinely need a different username/password than the broker's saved ones. Rather than force every VPN under one connection to share credentials (defeating the point of discovering them dynamically) or fail with no recourse, [`aggregator.peek()`](src/api/aggregator.py) takes optional `override_username`/`override_password` — used for that one call only, never persisted to `config/brokers.json`. The UI prompts for these inline whenever a peek failure looks credentials-related (see [`ui/README.md`](../ui/README.md)).

The override is wired identically across all five adapter types (Solace's SMF login, MQ's app credentials, Kafka's SASL credentials, RabbitMQ's management API credentials, ActiveMQ's Jolokia credentials), each with its own test in [`tests/test_aggregator.py`](tests/test_aggregator.py) proving the saved config's credentials are never touched when an override is given. Verified live end-to-end for both Solace (a genuinely unauthorized VPN, `testVPN`) and MQ (deliberately broke `app_password` in the config, confirmed the prompt appeared, retried with the correct password, confirmed it recovered, then restored the config) — a full break → prompt → recover cycle, not just a unit test. Kafka's local dev cluster runs PLAINTEXT with no auth to fail against, and RabbitMQ's/ActiveMQ's peek are unit-tested the same way; the wiring is identical to the verified two for whenever a real cluster/broker needs auth.

## Layout

```
src/api/
  models.py      the unified pydantic models (the actual shared schema, made real)
  config.py      loads/saves config/brokers.json, FIELD_SPECS (drives the "add broker" form), validation
  normalize.py   pure per-adapter-type -> unified-model conversion functions
  aggregator.py  BrokerRegistry — fans out to the five adapters, handles broker-down gracefully,
                 add/remove/test_connection for broker management
  main.py        FastAPI app and routes
config/brokers.json   broker inventory for local dev (see note above)
tests/                pytest suite: normalize.py against real adapter dataclasses, aggregator.py's
                      error handling and broker-management methods, config.py's validation/persistence,
                      TLS/SASL kwarg construction
```

## Running it locally

Needs all five adapters' dev brokers up first (Kafka, MQ, RabbitMQ, and ActiveMQ via this repo's `docker-compose.yml`; Solace per its own README).

```bash
cd api
python3 -m venv .venv
./.venv/bin/pip install -e ../adapters/kafka -e ../adapters/solace -e ../adapters/mq -e ../adapters/rabbitmq -e ../adapters/activemq -e ".[dev]"

./.venv/bin/uvicorn api.main:app --reload --port 8010
# NOTE: something on this machine's Docker networking already claims :8000
# (an IPv6 listener with nothing behind it) — use a different port, don't
# fight it, see git history if you need the story.

curl http://localhost:8010/api/brokers
curl http://localhost:8010/api/resources
curl "http://localhost:8010/api/resources/local-kafka:orders/messages?limit=5"

./.venv/bin/pytest
```

## Next step

Build the UI against this (React + Vite + TypeScript — see [`/ui/README.md`](../ui/README.md)). Longer-term, replace the live-passthrough aggregator with the real poller + time-series store pipeline once this needs to handle more than a handful of local brokers.
