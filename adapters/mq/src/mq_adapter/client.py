"""Talks to an IBM MQ queue manager's REST Admin API and normalizes what
it finds into the shared schema (see models.py / /docs/architecture.md
§3).

Queue depth/status comes via MQSC-over-REST (see mqsc.py for why: the
plain REST resource endpoints don't expose runtime attributes). No PCF
and no native MQI client (pymqi) — everything here is plain HTTPS/JSON,
same as the Kafka and Solace adapters' dependency profile.
"""

from __future__ import annotations

import datetime as _dt

import requests
import urllib3

from .health import classify_qmgr_status, classify_queue_depth
from .models import HealthCategory, HealthEvent, HealthSeverity, Resource
from .mqsc import parse_command_response

_DEFAULT_TIMEOUT_S = 10
# Any non-empty value works — the REST API only checks the header is
# present, not its content, as a CSRF mitigation for this simple case.
_CSRF_HEADER = {"ibm-mq-rest-csrf-token": "one-msg-ui"}


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


class MQAdapter:
    """One instance per queue manager, per the Broker shape in the shared model."""

    def __init__(self, broker_id: str, base_url: str, qmgr_name: str, username: str, password: str, verify_tls: bool = False):
        self.broker_id = broker_id
        self.base_url = base_url.rstrip("/")
        self.qmgr_name = qmgr_name
        self._session = requests.Session()
        self._session.auth = (username, password)
        self._session.verify = verify_tls  # dev queue managers commonly use self-signed certs
        if not verify_tls:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def close(self) -> None:
        self._session.close()

    # -- low-level helpers --------------------------------------------------

    def _run_mqsc(self, command: str) -> list[dict[str, str]]:
        resp = self._session.post(
            f"{self.base_url}/ibmmq/rest/v1/admin/action/qmgr/{self.qmgr_name}/mqsc",
            json={"type": "runCommand", "parameters": {"command": command}},
            headers=_CSRF_HEADER,
            timeout=_DEFAULT_TIMEOUT_S,
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("overallCompletionCode", 0) != 0:
            raise RuntimeError(f"MQSC command failed: {command!r} -> {body}")
        return parse_command_response(body.get("commandResponse", []))

    # -- resources ------------------------------------------------------------

    def get_resources(self, queue_pattern: str = "*") -> list[Resource]:
        rows = self._run_mqsc(f"DISPLAY QUEUE({queue_pattern}) TYPE(QLOCAL) CURDEPTH MAXDEPTH IPPROCS OPPROCS")
        resources = []
        for row in rows:
            name = row.get("QUEUE", "")
            resources.append(
                Resource(
                    id=f"{self.broker_id}:{self.qmgr_name}:{name}",
                    broker_id=self.broker_id,
                    namespace=self.qmgr_name,
                    name=name,
                    kind="queue",
                    depth_current=int(row["CURDEPTH"]) if "CURDEPTH" in row else None,
                    depth_max=int(row["MAXDEPTH"]) if "MAXDEPTH" in row else None,
                    consumer_lag=None,  # no per-consumer offset model in MQ
                    open_input_count=int(row["IPPROCS"]) if "IPPROCS" in row else None,
                    open_output_count=int(row["OPPROCS"]) if "OPPROCS" in row else None,
                    last_updated=_now_iso(),
                )
            )
        return resources

    # -- health -----------------------------------------------------------

    def get_health_events(self, resources: list[Resource] | None = None) -> list[HealthEvent]:
        qmgr_rows = self._run_mqsc("DISPLAY QMSTATUS STATUS")
        status = qmgr_rows[0].get("STATUS") if qmgr_rows else None
        connectivity = HealthEvent(
            broker_id=self.broker_id,
            severity=classify_qmgr_status(status),
            category=HealthCategory.CONNECTIVITY,
            message=f"queue manager {self.qmgr_name!r} status={status}",
            timestamp=_now_iso(),
        )

        resources = resources if resources is not None else self.get_resources()
        over_threshold = []
        for r in resources:
            if r.depth_current is None or not r.depth_max:
                continue
            severity, pct = classify_queue_depth(r.depth_current, r.depth_max)
            if severity != HealthSeverity.OK:
                over_threshold.append((r.name, pct, severity))

        if not over_threshold:
            capacity_severity = HealthSeverity.OK
            message = f"all {len(resources)} queue(s) under capacity threshold"
        else:
            worst = max(over_threshold, key=lambda t: t[1])
            capacity_severity = worst[2]
            names = ", ".join(f"{name} ({pct:.0f}%)" for name, pct, _ in over_threshold[:5])
            message = f"{len(over_threshold)}/{len(resources)} queue(s) over capacity threshold: {names}"

        capacity = HealthEvent(
            broker_id=self.broker_id,
            severity=capacity_severity,
            category=HealthCategory.CAPACITY,
            message=message,
            timestamp=_now_iso(),
        )
        return [connectivity, capacity]
