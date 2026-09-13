"""Pure health-classification logic, kept separate from client.py's HTTP
I/O so it's unit-testable without a live broker (see tests/test_health.py).
"""

from __future__ import annotations

from .models import HealthSeverity

_DEFAULT_WARN_PERCENT = 80
_CRITICAL_PERCENT = 95


def classify_queue_depth(current: int, max_length: int, warn_percent: float = _DEFAULT_WARN_PERCENT) -> tuple[HealthSeverity, float]:
    """Returns (severity, usage_pct) for a queue's depth vs. its
    `x-max-length` policy — same shape as MQ's classify_queue_depth and
    Solace's classify_spool_usage. A max_length of 0 (shouldn't happen,
    but config can be inconsistent) is treated as ok: no ceiling to be near.
    """
    if not max_length:
        return HealthSeverity.OK, 0.0
    usage_pct = current / max_length * 100
    if usage_pct >= _CRITICAL_PERCENT:
        return HealthSeverity.CRITICAL, usage_pct
    if usage_pct >= warn_percent:
        return HealthSeverity.WARN, usage_pct
    return HealthSeverity.OK, usage_pct


def classify_node_health(running: bool | None, mem_alarm: bool | None, disk_free_alarm: bool | None) -> HealthSeverity:
    """A node that isn't running is critical outright. A running node
    under a resource alarm (RabbitMQ throttles/blocks publishers when
    these trip) is a warning, not yet critical — publishers are degraded,
    not down.
    """
    if not running:
        return HealthSeverity.CRITICAL
    if mem_alarm or disk_free_alarm:
        return HealthSeverity.WARN
    return HealthSeverity.OK
