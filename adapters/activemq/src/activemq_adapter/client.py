"""Talks to an ActiveMQ Artemis broker's Jolokia endpoint (JMX-over-HTTP,
bundled with the broker's own web console and enabled by default — see
the startup log line "Artemis Jolokia REST API available at
http://.../console/jolokia") and normalizes what it finds into the shared
schema (see models.py / /docs/architecture.md §3).

Jolokia is the only thing this adapter needs — it covers monitoring
(broker/queue JMX MBeans) AND non-destructive message browsing (the queue
MBean's own `browse()` operation — see peek.py) in one plain-JSON REST
surface. No JMS/core client (or JMX-over-RMI) required at all, same
lightweight profile as the RabbitMQ adapter's management-API-only design.

Verified empirically against a real broker (see adapters/activemq/README.md):
- POST-with-mbean-in-body is used throughout rather than GET-with-mbean-
  in-the-URL-path — Jolokia's path-based addressing needs its own escaping
  for the quotes every Artemis ObjectName property value carries (e.g.
  `broker="0.0.0.0"`), and got the request badly wrong more than once
  during testing; the JSON-body form takes the exact canonical ObjectName
  string with no extra escaping needed.
- A single search with only `component=addresses,subcomponent=queues,*`
  (no `broker=` in the pattern at all) finds every queue MBean regardless
  of broker name — no need to discover the broker's own JMX name just to
  list queues. The *broker* MBean itself still needs its name discovered
  (via `broker=*`) since health lives there.
"""

from __future__ import annotations

import datetime as _dt

import requests

from .health import classify_broker_state, classify_usage
from .models import HealthCategory, HealthEvent, HealthSeverity, Resource

_DEFAULT_TIMEOUT_S = 10
_QUEUE_SEARCH_PATTERN = "org.apache.activemq.artemis:component=addresses,subcomponent=queues,*"
_BROKER_SEARCH_PATTERN = "org.apache.activemq.artemis:broker=*"


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


class ActiveMQAdapter:
    """One instance per broker. Artemis has no vhost/VPN-style multi-
    tenant scoping above addresses (unlike Solace/RabbitMQ) — a
    connection just discovers every address/queue its credentials can see.
    """

    def __init__(self, broker_id: str, base_url: str, username: str, password: str, verify_certificate: bool = True):
        self.broker_id = broker_id
        # base_url is the web console's own base (e.g. http://host:8161) —
        # the Jolokia path is appended here, same convention as Solace's
        # semp_url/RabbitMQ's api_url taking a bare base.
        self._jolokia_url = f"{base_url.rstrip('/')}/console/jolokia/"
        self._session = requests.Session()
        self._session.auth = (username, password)
        self._session.verify = verify_certificate
        self._broker_mbean: str | None = None  # discovered lazily, cached per instance

    def close(self) -> None:
        self._session.close()

    # -- low-level Jolokia helpers -------------------------------------------

    def _post(self, body: dict) -> dict:
        resp = self._session.post(self._jolokia_url, json=body, timeout=_DEFAULT_TIMEOUT_S)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != 200:
            raise RuntimeError(f"Jolokia request failed: {body} -> {data}")
        return data["value"]

    def _search(self, mbean_pattern: str) -> list[str]:
        return self._post({"type": "search", "mbean": mbean_pattern})

    def _read(self, mbean: str) -> dict:
        return self._post({"type": "read", "mbean": mbean})

    def _exec(self, mbean: str, operation: str, arguments: list | None = None) -> object:
        body = {"type": "exec", "mbean": mbean, "operation": operation}
        if arguments:
            body["arguments"] = arguments
        return self._post(body)

    def _broker_mbean_name(self) -> str:
        if self._broker_mbean is None:
            found = self._search(_BROKER_SEARCH_PATTERN)
            if not found:
                raise RuntimeError("no Artemis broker MBean found — is this really an Artemis broker?")
            self._broker_mbean = found[0]
        return self._broker_mbean

    # -- resources ------------------------------------------------------------

    def get_resources(self) -> list[Resource]:
        resources = []
        for mbean in self._search(_QUEUE_SEARCH_PATTERN):
            attrs = self._read(mbean)
            ring_size = attrs.get("RingSize", -1)
            address = attrs["Address"]
            name = attrs["Name"]
            resources.append(
                Resource(
                    id=f"{self.broker_id}:{address}:{name}",
                    broker_id=self.broker_id,
                    namespace=address,
                    name=name,
                    kind="queue",
                    depth_current=attrs.get("MessageCount"),
                    depth_max=ring_size if ring_size and ring_size > 0 else None,
                    consumer_lag=None,  # no per-consumer offset model, same as Solace/MQ/RabbitMQ
                    consumer_count=attrs.get("ConsumerCount"),
                    last_updated=_now_iso(),
                )
            )
        return resources

    # -- health -------------------------------------------------------------

    def get_health_events(self, resources: list[Resource] | None = None) -> list[HealthEvent]:
        broker = self._read(self._broker_mbean_name())

        events = [
            HealthEvent(
                broker_id=self.broker_id,
                severity=classify_broker_state(broker.get("Started"), broker.get("Active")),
                category=HealthCategory.CONNECTIVITY,
                message=f"broker {broker.get('Name')!r} started={broker.get('Started')} active={broker.get('Active')}",
                timestamp=_now_iso(),
            )
        ]

        # Broker-wide memory pressure — Artemis throttles/blocks producers
        # as AddressMemoryUsage approaches GlobalMaxSize, the closest
        # analogue to RabbitMQ's mem_alarm/Solace's spool-usage quota.
        used = broker.get("AddressMemoryUsage") or 0
        max_size = broker.get("GlobalMaxSize") or 0
        mem_severity, mem_pct = classify_usage(used, max_size)
        events.append(
            HealthEvent(
                broker_id=self.broker_id,
                severity=mem_severity,
                category=HealthCategory.CAPACITY,
                message=f"address memory usage {used}/{max_size} bytes ({mem_pct:.1f}%)",
                timestamp=_now_iso(),
            )
        )

        # Per-queue RingSize is opt-in, same as RabbitMQ's x-max-length —
        # most queues won't have one, so say that rather than reporting a
        # false "0/0 over threshold" as if every queue had been checked.
        resources = resources if resources is not None else self.get_resources()
        bounded = [r for r in resources if r.depth_max]
        over_threshold = []
        for r in bounded:
            severity, pct = classify_usage(r.depth_current or 0, r.depth_max)
            if severity != HealthSeverity.OK:
                over_threshold.append((r.name, pct, severity))

        if not bounded:
            depth_severity = HealthSeverity.OK
            depth_message = "no queue ring-size limits configured"
        elif not over_threshold:
            depth_severity = HealthSeverity.OK
            depth_message = f"all {len(bounded)} bounded queue(s) under capacity threshold"
        else:
            worst = max(over_threshold, key=lambda t: t[1])
            depth_severity = worst[2]
            names = ", ".join(f"{name} ({pct:.0f}%)" for name, pct, _ in over_threshold[:5])
            depth_message = f"{len(over_threshold)}/{len(bounded)} bounded queue(s) over capacity threshold: {names}"

        events.append(
            HealthEvent(broker_id=self.broker_id, severity=depth_severity, category=HealthCategory.CAPACITY, message=depth_message, timestamp=_now_iso())
        )
        return events
