"""Shared normalized shapes this adapter produces.

Mirrors the schema defined in /docs/architecture.md §3 ("Unified data
model") and §3.1 ("Message browsing/peek"). Artemis's queue-to-address
binding (address -> queue) is kept explicit via `namespace`, same shape
as Solace's VPN, MQ's queue manager, and RabbitMQ's vhost — never
flattened away. Unlike those three, Artemis has no separate multi-tenant
scoping layer above addresses (one broker connection just discovers
every address/queue it can see — there's no vhost/VPN-style filter to
offer here).
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
    system_type: str  # "activemq" for this adapter
    name: str
    environment: str  # prod | uat | dev
    status: BrokerStatus


@dataclass
class Resource:
    """The "queue-ish" thing — one per Artemis queue bound to an address.

    depth_current is `MessageCount`. depth_max is the queue's `RingSize`
    (a hard per-queue message-count cap), when one is actually set —
    most Artemis queues don't set one (flow control is normally done at
    the *address* level via max-size-bytes, a memory limit shared across
    everything bound to that address, not a per-queue message count), so
    this is None far more often than not — same posture as RabbitMQ's
    mostly-unbounded queues. consumer_lag stays None: no per-consumer
    offset model, same as Solace/MQ/RabbitMQ.
    """

    id: str
    broker_id: str
    namespace: str  # the address this queue is bound to
    name: str
    kind: str = "queue"
    depth_current: int | None = None  # MessageCount
    depth_max: int | None = None  # RingSize, when set (> 0)
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

    The queue MBean's own `browse()` operation, invoked over Jolokia,
    does this directly — a real broker-side browse, not a workaround
    (confirmed live: queue MessageCount was unchanged after browsing all
    4 test messages, repeatable like Kafka's/Solace's/RabbitMQ's peek,
    not a one-shot cursor like MQ's).

    Real limitation, not worked around: `browse()`'s JSON only exposes a
    `text` field for TEXT-type messages (JMS TextMessage / Core message
    with a text body — what every adapter's own seed script publishes).
    A BytesMessage/ObjectMessage/StreamMessage has no equivalent field in
    this API surface, so its body isn't recoverable through Jolokia alone
    — see peek.py and adapters/activemq/README.md.
    """

    resource_id: str
    message_id: str | None
    timestamp: str | int | None
    topic: str | None = None  # the address the message arrived on —
    # Artemis's closest analogue to Kafka's topic / Solace's topic
    # subscription match / RabbitMQ's routing key
    headers: dict[str, str] = field(default_factory=dict)
    body_preview: str = ""
    size_bytes: int = 0
