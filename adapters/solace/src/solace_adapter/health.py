"""Pure health-classification logic, kept separate from client.py's SEMP
I/O so it's unit-testable without a live broker (see tests/test_health.py).
"""

from __future__ import annotations

from .models import HealthSeverity

_DEFAULT_WARN_PERCENT = 80
_CRITICAL_PERCENT = 95


def classify_spool_usage(used: float, max_quota: float, warn_percent: float = _DEFAULT_WARN_PERCENT) -> tuple[HealthSeverity, float]:
    """Returns (severity, usage_pct). `warn_percent` should come from the
    VPN's own configured eventMsgSpoolUsageThreshold when available — using
    the broker's own operational threshold rather than a value this
    adapter invents. A max_quota of 0 (unbounded/unset) is treated as ok:
    there's no ceiling to be near.
    """
    if not max_quota:
        return HealthSeverity.OK, 0.0
    usage_pct = used / max_quota * 100
    if usage_pct >= _CRITICAL_PERCENT:
        return HealthSeverity.CRITICAL, usage_pct
    if usage_pct >= warn_percent:
        return HealthSeverity.WARN, usage_pct
    return HealthSeverity.OK, usage_pct


def classify_vpn_connectivity(state: str | None, enabled: bool | None) -> HealthSeverity:
    return HealthSeverity.OK if state == "up" and enabled else HealthSeverity.CRITICAL
