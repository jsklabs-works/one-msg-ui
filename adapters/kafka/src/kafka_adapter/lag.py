"""Pure lag-calculation logic, kept separate from client.py's Kafka I/O so
it's trivially unit-testable (see tests/test_lag.py) without a live broker.
"""

from __future__ import annotations


def compute_lag(log_end_offset: int | None, committed_offset: int | None) -> int | None:
    """log-end-offset minus committed-offset, clamped at 0.

    Returns None (not zero) when either input is unknown — e.g. no
    committed offset yet — per the "no lag data" vs. "zero lag"
    distinction in adapters/kafka/README.md's known gotchas. Callers
    should propagate that None rather than coercing it to 0.
    """
    if log_end_offset is None or committed_offset is None:
        return None
    return max(log_end_offset - committed_offset, 0)


def sum_topic_lag(lags: list[int | None]) -> int | None:
    """Sum per-partition lag values for a topic across one or more
    consumer groups. Returns None only if every value is None (no group
    has consumed this topic at all) — a mix of None and real values
    treats the None entries as simply not contributing, rather than
    poisoning the whole sum, since a partition can legitimately have no
    committed offset yet while its siblings do.
    """
    known = [lag for lag in lags if lag is not None]
    if not known:
        return None
    return sum(known)
