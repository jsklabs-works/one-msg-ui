"""Shared normalized shapes this adapter produces.

Mirrors the schema defined in /docs/architecture.md §3 ("Unified data
model") and §3.1 ("Message browsing/peek"). Solace's two-level scoping
(Message VPN -> queue/topic) is kept explicit via `namespace` rather than
flattened away, per the architecture doc's explicit warning about that.
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
    system_type: str  # "solace" for this adapter
    name: str
    environment: str  # prod | uat | dev
    status: BrokerStatus


@dataclass
class Resource:
    """The "queue-ish" thing — one per Solace queue.

    depth_current is spooled message count; depth_max is the queue's
    configured spool quota (bytes/MB, not a message-count ceiling — see
    architecture.md §3's note that Solace's depth_max means something
    structurally different from MQ's MAXDEPTH). consumer_lag stays None:
    Solace queues have no per-consumer offset model.
    """

    id: str
    broker_id: str
    namespace: str  # Message VPN name — kept explicit, never flattened
    name: str
    kind: str = "queue"
    depth_current: int | None = None  # spooledMsgCount
    depth_max: int | None = None  # maxMsgSpoolUsage (MB quota)
    consumer_lag: int | None = None
    spooled_bytes: int | None = None
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

    Body access requires a real client-protocol connection (the official
    solace-pubsubplus Browser API — see peek.py); SEMP alone only ever
    yields message metadata, never the body (verified empirically against
    a real broker — see adapters/solace/README.md).
    """

    resource_id: str
    message_id: str | None
    timestamp: int | None
    headers: dict[str, str] = field(default_factory=dict)
    body_preview: str = ""
    size_bytes: int = 0
