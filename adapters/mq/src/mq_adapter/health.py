"""Pure health-classification logic, kept separate from client.py's REST
I/O so it's unit-testable without a live queue manager (see
tests/test_health.py).
"""

from __future__ import annotations

from .models import HealthSeverity

_DEFAULT_WARN_PERCENT = 80
_CRITICAL_PERCENT = 95


def classify_queue_depth(curdepth: int, maxdepth: int, warn_percent: float = _DEFAULT_WARN_PERCENT) -> tuple[HealthSeverity, float]:
    """Returns (severity, usage_pct) for a queue's depth vs. its configured
    ceiling. maxdepth of 0 (shouldn't happen in practice, but MQ config
    can be inconsistent) is treated as ok: there's no ceiling to be near.
    """
    if not maxdepth:
        return HealthSeverity.OK, 0.0
    usage_pct = curdepth / maxdepth * 100
    if usage_pct >= _CRITICAL_PERCENT:
        return HealthSeverity.CRITICAL, usage_pct
    if usage_pct >= warn_percent:
        return HealthSeverity.WARN, usage_pct
    return HealthSeverity.OK, usage_pct


def classify_qmgr_status(status: str | None) -> HealthSeverity:
    """MQ queue manager status is one of "running", "starting", "ending",
    "ended normally", "ended immediately", "ended unexpectedly", etc.
    Only a clean "running" is ok; anything else — including transitional
    states — is at least a warning, since a monitoring view should surface
    "not fully up" rather than silently treat it as fine.
    """
    if status is None:
        return HealthSeverity.CRITICAL
    normalized = status.strip().lower()
    if normalized == "running":
        return HealthSeverity.OK
    if normalized in ("starting", "ending immediately", "ending pre-emptively"):
        return HealthSeverity.WARN
    return HealthSeverity.CRITICAL
