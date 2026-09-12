"""Replication/ISR health, computed from the Admin API's topic metadata.

Turns out this doesn't need JMX at all: `KafkaAdminClient.describe_topics()`
already returns each partition's `replicas`, `isr`, and `offline_replicas`
straight from the Metadata API — the same data a broker-side JMX
UnderReplicatedPartitions metric would derive from. JMX is only needed for
things the Metadata API doesn't carry (request latency percentiles,
handler idle ratio, etc.) — not for this. See client.py's
`get_replication_health_events()`.

Kept as pure, kafka-client-free logic here so it's unit-testable without a
broker — see tests/test_replication.py.
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import HealthSeverity


@dataclass(frozen=True)
class PartitionReplicationStatus:
    topic: str
    partition: int
    replicas: list[int]
    isr: list[int]
    offline_replicas: list[int]

    @property
    def is_offline(self) -> bool:
        return len(self.offline_replicas) > 0

    @property
    def is_under_replicated(self) -> bool:
        # Under-replicated is the broader condition; an offline replica is
        # also under-replicated but gets reported as the more severe case.
        return len(self.isr) < len(self.replicas)


@dataclass(frozen=True)
class ReplicationSummary:
    severity: HealthSeverity
    message: str
    offline_partitions: list[PartitionReplicationStatus]
    under_replicated_partitions: list[PartitionReplicationStatus]


_MAX_NAMED_PARTITIONS_IN_MESSAGE = 5


def _format_partition_list(statuses: list[PartitionReplicationStatus]) -> str:
    names = [f"{s.topic}-{s.partition}" for s in statuses[:_MAX_NAMED_PARTITIONS_IN_MESSAGE]]
    text = ", ".join(names)
    if len(statuses) > _MAX_NAMED_PARTITIONS_IN_MESSAGE:
        text += f", and {len(statuses) - _MAX_NAMED_PARTITIONS_IN_MESSAGE} more"
    return text


def summarize_replication(statuses: list[PartitionReplicationStatus]) -> ReplicationSummary:
    """Classify a cluster's partitions into ok/warn/critical.

    offline_replicas non-empty on any partition -> critical (data at risk
    of unavailability). Otherwise any under-replicated partition -> warn
    (redundancy degraded but still serving). Otherwise ok.
    """
    offline = [s for s in statuses if s.is_offline]
    under_replicated = [s for s in statuses if s.is_under_replicated and not s.is_offline]

    total = len(statuses)
    if offline:
        severity = HealthSeverity.CRITICAL
        message = f"{len(offline)}/{total} partition(s) have offline replicas: {_format_partition_list(offline)}"
    elif under_replicated:
        severity = HealthSeverity.WARN
        message = f"{len(under_replicated)}/{total} partition(s) under-replicated: {_format_partition_list(under_replicated)}"
    else:
        severity = HealthSeverity.OK
        message = f"all {total} partition(s) fully replicated"

    return ReplicationSummary(
        severity=severity,
        message=message,
        offline_partitions=offline,
        under_replicated_partitions=under_replicated,
    )
