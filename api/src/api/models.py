"""The unified data model, made real — see /docs/architecture.md §3.

Each adapter (kafka_adapter, solace_adapter, mq_adapter) has its own
dataclasses with broker-specific extra fields. This layer's job is
exactly what the architecture doc describes: normalize each adapter's
output into these shared shapes before anything downstream (the UI) sees
it, tagging every record with `system_type` so the UI can decide what to
show per system without needing to know adapter internals.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

SystemType = Literal["kafka", "solace", "mq"]
BrokerStatus = Literal["up", "down", "degraded"]
HealthSeverity = Literal["ok", "warn", "critical"]
HealthCategory = Literal["connectivity", "capacity", "replication", "spool"]


class Broker(BaseModel):
    id: str
    system_type: SystemType
    name: str
    environment: str
    status: BrokerStatus


class Resource(BaseModel):
    id: str
    broker_id: str
    system_type: SystemType
    namespace: str
    name: str
    kind: str
    depth_current: int | None = None
    depth_max: int | None = None
    consumer_lag: int | None = None
    last_updated: str | None = None


class HealthEvent(BaseModel):
    broker_id: str
    system_type: SystemType
    severity: HealthSeverity
    category: HealthCategory
    message: str
    timestamp: str


class MessageSample(BaseModel):
    resource_id: str
    message_id: str | None = None
    timestamp: str | int | None = None
    headers: dict[str, str] = {}
    body_preview: str = ""
    size_bytes: int = 0


class ConsumerGroupPartition(BaseModel):
    topic: str
    partition: int
    log_end_offset: int
    committed_offset: int | None
    lag: int | None


class ConsumerGroup(BaseModel):
    id: str
    broker_id: str
    group_name: str
    state: str
    member_count: int
    partitions: list[ConsumerGroupPartition]
