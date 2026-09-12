"""Shared normalized shapes this adapter produces.

Mirrors the schema defined in /docs/architecture.md §3 ("Unified data
model") and §3.1 ("Message browsing/peek"). Kept as plain dataclasses
with no Kafka-specific fields leaking through — everything Kafka-native
(offsets, ISR, etc.) gets translated before it reaches these shapes.
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
    system_type: str  # "kafka" for this adapter
    name: str
    environment: str  # prod | uat | dev
    status: BrokerStatus


@dataclass
class PartitionLag:
    """One partition's contribution to a consumer group's lag.

    `lag` is None (not zero) when it genuinely couldn't be computed —
    e.g. no committed offset yet — per the "no lag data" vs. "zero lag"
    distinction called out in the adapter README's known gotchas.
    """

    topic: str
    partition: int
    log_end_offset: int
    committed_offset: int | None
    lag: int | None


@dataclass
class ConsumerGroup:
    id: str
    broker_id: str
    group_name: str
    state: str
    member_count: int
    partitions: list[PartitionLag] = field(default_factory=list)


@dataclass
class Resource:
    """The "queue-ish" thing — one per Kafka topic.

    depth_current / depth_max stay None: Kafka has no native depth
    concept. consumer_lag is the sum of lag across all consumer groups'
    partitions for this topic (None if no group has consumed it yet,
    distinct from a genuinely-zero lag).
    """

    id: str
    broker_id: str
    namespace: str  # cluster id/name
    name: str
    kind: str = "topic"
    depth_current: int | None = None
    depth_max: int | None = None
    consumer_lag: int | None = None
    partition_count: int = 0
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
    """Non-destructive peek at a topic's messages — see architecture.md §3.1."""

    resource_id: str
    partition: int
    offset: int
    timestamp: int | None
    headers: dict[str, str]
    body_preview: str
    size_bytes: int
