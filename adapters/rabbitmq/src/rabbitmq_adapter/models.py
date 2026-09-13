"""Shared normalized shapes this adapter produces.

Mirrors the schema defined in /docs/architecture.md §3 ("Unified data
model") and §3.1 ("Message browsing/peek"). RabbitMQ's virtual-host
scoping (vhost -> queue) is kept explicit via `namespace`, same as
Solace's Message VPN and MQ's queue manager — never flattened away, per
the architecture doc's explicit warning about that.
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
    system_type: str  # "rabbitmq" for this adapter
    name: str
    environment: str  # prod | uat | dev
    status: BrokerStatus


@dataclass
class Resource:
    """The "queue-ish" thing — one per RabbitMQ queue (exchanges route
    messages but don't hold them, so they're not resources here, same
    reasoning as why Kafka doesn't surface consumer groups as resources).

    depth_current is `messages` (ready + unacknowledged). depth_max is
    the queue's `x-max-length` policy in messages, when one is set — most
    RabbitMQ queues have no length limit at all (unlike MQ, where
    MAXDEPTH always exists), so this is None far more often than the
    other adapters' depth_max. consumer_lag stays None: no per-consumer
    offset model, same as Solace/MQ.
    """

    id: str
    broker_id: str
    namespace: str  # vhost name (default vhost is "/")
    name: str
    kind: str = "queue"
    depth_current: int | None = None  # messages (ready + unacked)
    depth_max: int | None = None  # arguments["x-max-length"], when set
    consumer_lag: int | None = None
    consumer_count: int | None = None
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

    The management API's own `get` endpoint with `ackmode:
    "ack_requeue_true"` does this directly: it delivers the message *and*
    immediately requeues it, so nothing is actually consumed off the
    queue — no separate client-protocol connection needed, unlike Solace
    (see peek.py).
    """

    resource_id: str
    message_id: str | None
    timestamp: str | int | None
    topic: str | None = None  # the routing key the message arrived with —
    # RabbitMQ's closest analogue to Kafka's topic / Solace's topic
    # subscription match: the actual thing that routed it onto this queue
    headers: dict[str, str] = field(default_factory=dict)
    body_preview: str = ""
    size_bytes: int = 0
