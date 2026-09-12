"""Converts each adapter's own dataclasses into the unified pydantic
models in models.py. Pure, no I/O — unit-tested in tests/test_normalize.py
by feeding in adapter dataclass instances directly, no live broker needed.

This is the actual "normalize to common schema" step from the
architecture.md §4 diagram, made concrete.
"""

from __future__ import annotations

from . import models


def kafka_resource(r, broker_id: str) -> models.Resource:
    return models.Resource(
        id=r.id,
        broker_id=broker_id,
        system_type="kafka",
        namespace=r.namespace,
        name=r.name,
        kind=r.kind,
        depth_current=r.depth_current,
        depth_max=r.depth_max,
        consumer_lag=r.consumer_lag,
        last_updated=r.last_updated,
    )


def kafka_health_event(h, broker_id: str) -> models.HealthEvent:
    return models.HealthEvent(
        broker_id=broker_id,
        system_type="kafka",
        severity=h.severity.value if hasattr(h.severity, "value") else h.severity,
        category=h.category.value if hasattr(h.category, "value") else h.category,
        message=h.message,
        timestamp=h.timestamp,
    )


def kafka_consumer_group(g, broker_id: str) -> models.ConsumerGroup:
    return models.ConsumerGroup(
        id=g.id,
        broker_id=broker_id,
        group_name=g.group_name,
        state=g.state,
        member_count=g.member_count,
        partitions=[
            models.ConsumerGroupPartition(
                topic=p.topic,
                partition=p.partition,
                log_end_offset=p.log_end_offset,
                committed_offset=p.committed_offset,
                lag=p.lag,
            )
            for p in g.partitions
        ],
    )


def kafka_message_sample(m) -> models.MessageSample:
    return models.MessageSample(
        resource_id=m.resource_id,
        message_id=f"{m.partition}:{m.offset}",
        timestamp=m.timestamp,
        headers=m.headers,
        body_preview=m.body_preview,
        size_bytes=m.size_bytes,
    )


def solace_resource(r, broker_id: str) -> models.Resource:
    return models.Resource(
        id=r.id,
        broker_id=broker_id,
        system_type="solace",
        namespace=r.namespace,
        name=r.name,
        kind=r.kind,
        depth_current=r.depth_current,
        depth_max=r.depth_max,
        consumer_lag=r.consumer_lag,
        last_updated=r.last_updated,
    )


def solace_health_event(h, broker_id: str) -> models.HealthEvent:
    return models.HealthEvent(
        broker_id=broker_id,
        system_type="solace",
        severity=h.severity.value if hasattr(h.severity, "value") else h.severity,
        category=h.category.value if hasattr(h.category, "value") else h.category,
        message=h.message,
        timestamp=h.timestamp,
    )


def solace_message_sample(m) -> models.MessageSample:
    return models.MessageSample(
        resource_id=m.resource_id,
        message_id=m.message_id,
        timestamp=m.timestamp,
        headers=m.headers,
        body_preview=m.body_preview,
        size_bytes=m.size_bytes,
    )


def mq_resource(r, broker_id: str) -> models.Resource:
    return models.Resource(
        id=r.id,
        broker_id=broker_id,
        system_type="mq",
        namespace=r.namespace,
        name=r.name,
        kind=r.kind,
        depth_current=r.depth_current,
        depth_max=r.depth_max,
        consumer_lag=r.consumer_lag,
        last_updated=r.last_updated,
    )


def mq_health_event(h, broker_id: str) -> models.HealthEvent:
    return models.HealthEvent(
        broker_id=broker_id,
        system_type="mq",
        severity=h.severity.value if hasattr(h.severity, "value") else h.severity,
        category=h.category.value if hasattr(h.category, "value") else h.category,
        message=h.message,
        timestamp=h.timestamp,
    )


def mq_message_sample(m) -> models.MessageSample:
    return models.MessageSample(
        resource_id=m.resource_id,
        message_id=m.message_id,
        timestamp=m.timestamp,
        headers=m.headers,
        body_preview=m.body_preview,
        size_bytes=m.size_bytes,
    )
