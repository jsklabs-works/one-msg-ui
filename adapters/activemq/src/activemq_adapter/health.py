"""Pure health-classification logic, kept separate from client.py's HTTP
I/O so it's unit-testable without a live broker (see tests/test_health.py).
"""

from __future__ import annotations

from .models import HealthSeverity

_DEFAULT_WARN_PERCENT = 80
_CRITICAL_PERCENT = 95


def classify_usage(used: float, max_value: float, warn_percent: float = _DEFAULT_WARN_PERCENT) -> tuple[HealthSeverity, float]:
    """Returns (severity, usage_pct) for a value against a ceiling — used
    for both broker-wide memory usage (AddressMemoryUsage/GlobalMaxSize)
    and a queue's depth vs. its RingSize. Same shape as every other
    adapter's classify_*_usage/depth helper. A max_value of 0 (unbounded/
    unset) is treated as ok: there's no ceiling to be near.
    """
    if not max_value:
        return HealthSeverity.OK, 0.0
    usage_pct = used / max_value * 100
    if usage_pct >= _CRITICAL_PERCENT:
        return HealthSeverity.CRITICAL, usage_pct
    if usage_pct >= warn_percent:
        return HealthSeverity.WARN, usage_pct
    return HealthSeverity.OK, usage_pct


def classify_broker_state(started: bool | None, active: bool | None) -> HealthSeverity:
    """A broker that isn't Started is critical outright. Started-but-not-
    Active means a backup/passive node in a replicated pair waiting to
    take over — not serving traffic yet, but not broken either: a warning.
    """
    if not started:
        return HealthSeverity.CRITICAL
    if not active:
        return HealthSeverity.WARN
    return HealthSeverity.OK
