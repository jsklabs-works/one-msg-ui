"""Talks to a RabbitMQ broker's management HTTP API (the
`rabbitmq_management` plugin, enabled by default in the `*-management`
Docker images) and normalizes what it finds into the shared schema (see
models.py / /docs/architecture.md §3).

The management API is the only thing this adapter needs — it covers
monitoring (queues, nodes, vhosts) AND non-destructive message browsing
(see peek.py's `ackmode: ack_requeue_true`) in one plain-JSON REST
surface. No AMQP client (pika, etc.) required at all, which makes this
adapter's dependency profile even lighter than Solace's (whose peek
still needs a real client-protocol connection — see
adapters/solace/README.md for why).
"""

from __future__ import annotations

import datetime as _dt
from urllib.parse import quote

import requests

from .health import classify_node_health, classify_queue_depth
from .models import HealthCategory, HealthEvent, HealthSeverity, Resource

_DEFAULT_TIMEOUT_S = 10


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _quote(name: str) -> str:
    # RabbitMQ's management API expects vhost/queue names URL-encoded as a
    # single path segment — notably the default vhost "/" becomes "%2F",
    # not an extra path separator.
    return quote(name, safe="")


def resource_id(broker_id: str, vhost: str, name: str) -> str:
    """The `id` scheme every other adapter uses is `broker:namespace:name`
    — but RabbitMQ's default vhost is *literally* "/", and a raw "/"
    inside this app's `/api/resources/{resource_id}` route doesn't
    survive routing (confirmed live: a percent-encoded %2F is decoded by
    the ASGI server before Starlette splits the path into segments, so
    the single `{resource_id}` parameter never matches and the request
    404s). Escape just that one well-known value; every other vhost name
    passes through unchanged, and `Resource.namespace` itself still
    reports the real "/" — only the opaque id string is affected.
    """
    vhost_part = "__default__" if vhost == "/" else vhost
    return f"{broker_id}:{vhost_part}:{name}"


class RabbitMQAdapter:
    """One instance per broker. `vhost` is an optional *filter*, same
    pattern as the Solace adapter's `vpn_name`: unset means "every vhost
    these credentials can see", discovered dynamically via
    `GET /api/vhosts` — not hardcoded to one at connection time.
    """

    def __init__(
        self,
        broker_id: str,
        base_url: str,
        username: str,
        password: str,
        vhost: str | None = None,
        verify_certificate: bool = True,
    ):
        self.broker_id = broker_id
        self.base_url = base_url.rstrip("/")
        self.vhost = vhost or None  # "" from a form field means "no filter", same as None
        self._session = requests.Session()
        self._session.auth = (username, password)
        self._session.verify = verify_certificate

    def close(self) -> None:
        self._session.close()

    # -- low-level helpers --------------------------------------------------

    def _get(self, path: str) -> dict | list:
        resp = self._session.get(f"{self.base_url}{path}", timeout=_DEFAULT_TIMEOUT_S)
        resp.raise_for_status()
        return resp.json()

    def _list_vhost_names(self) -> list[str]:
        if self.vhost:
            return [self.vhost]
        vhosts = self._get("/api/vhosts")
        return [v["name"] for v in vhosts]

    # -- resources ------------------------------------------------------------

    def get_resources(self) -> list[Resource]:
        resources = []
        for vhost in self._list_vhost_names():
            queues = self._get(f"/api/queues/{_quote(vhost)}")
            for q in queues:
                name = q["name"]
                max_length = (q.get("arguments") or {}).get("x-max-length")
                resources.append(
                    Resource(
                        id=resource_id(self.broker_id, vhost, name),
                        broker_id=self.broker_id,
                        namespace=vhost,
                        name=name,
                        kind="queue",
                        depth_current=q.get("messages"),
                        depth_max=max_length,
                        consumer_lag=None,  # no per-consumer offset model, same as Solace/MQ
                        consumer_count=q.get("consumers"),
                        last_updated=_now_iso(),
                    )
                )
        return resources

    # -- health -------------------------------------------------------------

    def get_health_events(self, resources: list[Resource] | None = None) -> list[HealthEvent]:
        events: list[HealthEvent] = []

        for node in self._get("/api/nodes"):
            events.append(
                HealthEvent(
                    broker_id=self.broker_id,
                    severity=classify_node_health(node.get("running"), node.get("mem_alarm"), node.get("disk_free_alarm")),
                    category=HealthCategory.CONNECTIVITY,
                    message=(
                        f"node {node.get('name')} running={node.get('running')} "
                        f"mem_alarm={node.get('mem_alarm')} disk_free_alarm={node.get('disk_free_alarm')}"
                    ),
                    timestamp=_now_iso(),
                )
            )

        resources = resources if resources is not None else self.get_resources()
        # Most RabbitMQ queues have no length limit at all (x-max-length is
        # opt-in, unlike MQ's always-present MAXDEPTH) — only judge the
        # ones that actually have one configured.
        bounded = [r for r in resources if r.depth_max]
        over_threshold = []
        for r in bounded:
            severity, pct = classify_queue_depth(r.depth_current or 0, r.depth_max)
            if severity != HealthSeverity.OK:
                over_threshold.append((r.name, pct, severity))

        if not bounded:
            capacity_severity = HealthSeverity.OK
            message = "no queue length limits (x-max-length) configured"
        elif not over_threshold:
            capacity_severity = HealthSeverity.OK
            message = f"all {len(bounded)} bounded queue(s) under capacity threshold"
        else:
            worst = max(over_threshold, key=lambda t: t[1])
            capacity_severity = worst[2]
            names = ", ".join(f"{name} ({pct:.0f}%)" for name, pct, _ in over_threshold[:5])
            message = f"{len(over_threshold)}/{len(bounded)} bounded queue(s) over capacity threshold: {names}"

        events.append(
            HealthEvent(
                broker_id=self.broker_id,
                severity=capacity_severity,
                category=HealthCategory.CAPACITY,
                message=message,
                timestamp=_now_iso(),
            )
        )
        return events
