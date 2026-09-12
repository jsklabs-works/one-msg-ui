"""Shared normalized shapes this adapter produces.

Mirrors the schema defined in /docs/architecture.md §3 ("Unified data
model") and §3.1 ("Message browsing/peek").
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class BrokerStatus(str, Enum):
    UP = "up"
    DOWN = "down"
    DEGRADED = "degraded"


class HealthSeverity(str, Enum):
    OK = "ok"
    WARN = "warn"
    CRITICAL = "critical"


class HealthCategory(str, Enum):
    CONNECTIVITY = "connectivity"
    CAPACITY = "capacity"
    REPLICATION = "replication"
    SPOOL = "spool"


@dataclass
class Broker:
    id: str
    system_type: str  # "mq" for this adapter
    name: str
    environment: str  # prod | uat | dev
    status: BrokerStatus


@dataclass
class Resource:
    """The "queue-ish" thing — one per MQ local queue.

    depth_current is CURDEPTH, depth_max is MAXDEPTH — MQ's depth concept
    is the most literal of the three systems this project covers.
    consumer_lag stays None: no per-consumer offset model in MQ.
    """

    id: str
    broker_id: str
    namespace: str  # queue manager name
    name: str
    kind: str = "queue"
    depth_current: int | None = None  # CURDEPTH
    depth_max: int | None = None  # MAXDEPTH
    consumer_lag: int | None = None
    open_input_count: int | None = None
    open_output_count: int | None = None
    last_updated: str | None = None


@dataclass
class HealthEvent:
    broker_id: str
    severity: HealthSeverity
    category: HealthCategory
    message: str
    timestamp: str


@dataclass
class MessageSample:
    """Non-destructive peek at a queue's messages — architecture.md §3.1.

    Uses the REST Messaging API's browse mode (see peek.py) — pure HTTP,
    no PCF or native client needed.
    """

    resource_id: str
    message_id: str | None
    timestamp: str | None
    headers: dict[str, str] = field(default_factory=dict)
    body_preview: str = ""
    size_bytes: int = 0
