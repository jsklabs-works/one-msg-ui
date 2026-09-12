"""Talks to a Solace PubSub+ broker's SEMP v2 **monitor** API and
normalizes what it finds into the shared schema (see models.py /
/docs/architecture.md §3).

SEMP v2 monitor is the only source needed for Resource/HealthEvent — it's
plain JSON over REST, no client-protocol connection required. Message
*bodies* are a different story: verified empirically against a real
broker that SEMP's queue message-metadata endpoints
(`/queues/{q}/msgs[/​{id}]`) return size/timestamp/id but never the
payload — see peek.py, which uses the real messaging client instead.
"""

from __future__ import annotations

import datetime as _dt

import requests

from .health import classify_spool_usage, classify_vpn_connectivity
from .models import HealthCategory, HealthEvent, Resource

_DEFAULT_TIMEOUT_S = 10
_PAGE_SIZE = 100


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


class SolaceAdapter:
    """One instance per broker, scoped to a single Message VPN — Solace's
    two-level scoping (VPN -> queue/topic) is why `vpn_name` is required
    rather than optional, per architecture.md's explicit warning against
    flattening that away.
    """

    def __init__(self, broker_id: str, base_url: str, vpn_name: str, username: str, password: str):
        self.broker_id = broker_id
        self.base_url = base_url.rstrip("/")
        self.vpn_name = vpn_name
        self._session = requests.Session()
        self._session.auth = (username, password)

    def close(self) -> None:
        self._session.close()

    # -- low-level SEMP helpers -------------------------------------------

    def _get(self, path: str, params: dict | None = None) -> dict:
        resp = self._session.get(f"{self.base_url}{path}", params=params, timeout=_DEFAULT_TIMEOUT_S)
        resp.raise_for_status()
        return resp.json()

    def _get_all_pages(self, path: str, params: dict | None = None) -> list[dict]:
        """SEMP v2 pages long collections via meta.paging.cursorQuery; follow
        it until the response stops carrying one.
        """
        items: list[dict] = []
        query = dict(params or {})
        query.setdefault("count", _PAGE_SIZE)
        while True:
            body = self._get(path, params=query)
            items.extend(body.get("data", []))
            cursor = body.get("meta", {}).get("paging", {}).get("cursorQuery")
            if not cursor:
                break
            query = dict(query)
            query["cursor"] = cursor
        return items

    # -- resources ----------------------------------------------------------

    def get_resources(self) -> list[Resource]:
        queues = self._get_all_pages(f"/SEMP/v2/monitor/msgVpns/{self.vpn_name}/queues")
        resources = []
        for q in queues:
            name = q["queueName"]
            resources.append(
                Resource(
                    id=f"{self.broker_id}:{self.vpn_name}:{name}",
                    broker_id=self.broker_id,
                    namespace=self.vpn_name,
                    name=name,
                    kind="queue",
                    depth_current=q.get("spooledMsgCount"),
                    depth_max=q.get("maxMsgSpoolUsage"),
                    consumer_lag=None,  # no per-consumer offset model for Solace queues
                    spooled_bytes=q.get("spooledByteCount"),
                    last_updated=_now_iso(),
                )
            )
        return resources

    # -- health -------------------------------------------------------------

    def get_health_events(self) -> list[HealthEvent]:
        vpn = self._get(f"/SEMP/v2/monitor/msgVpns/{self.vpn_name}")["data"]

        connectivity = HealthEvent(
            broker_id=self.broker_id,
            severity=classify_vpn_connectivity(vpn.get("state"), vpn.get("enabled")),
            category=HealthCategory.CONNECTIVITY,
            message=f"VPN {self.vpn_name!r} state={vpn.get('state')} enabled={vpn.get('enabled')}",
            timestamp=_now_iso(),
        )

        max_spool = vpn.get("maxMsgSpoolUsage") or 0
        used_spool = vpn.get("msgSpoolUsage") or 0
        # Use the VPN's own configured event threshold rather than a
        # hardcoded number — same operational convention the broker itself
        # uses to raise its own spool-usage events.
        warn_at = vpn.get("eventMsgSpoolUsageThreshold", {}).get("setPercent", 80)
        severity, usage_pct = classify_spool_usage(used_spool, max_spool, warn_percent=warn_at)

        spool = HealthEvent(
            broker_id=self.broker_id,
            severity=severity,
            category=HealthCategory.SPOOL,
            message=f"spool usage {used_spool}/{max_spool} MB ({usage_pct:.1f}%) across VPN {self.vpn_name!r}",
            timestamp=_now_iso(),
        )

        return [connectivity, spool]
