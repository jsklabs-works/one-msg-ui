from kafka_adapter.models import HealthSeverity
from kafka_adapter.replication import PartitionReplicationStatus, summarize_replication


def _status(topic="orders", partition=0, replicas=(1, 2, 3), isr=(1, 2, 3), offline=()):
    return PartitionReplicationStatus(
        topic=topic, partition=partition, replicas=list(replicas), isr=list(isr), offline_replicas=list(offline)
    )


def test_fully_replicated_is_ok():
    statuses = [_status(partition=0), _status(partition=1)]
    summary = summarize_replication(statuses)
    assert summary.severity == HealthSeverity.OK
    assert summary.offline_partitions == []
    assert summary.under_replicated_partitions == []
    assert "2" in summary.message


def test_under_replicated_partition_is_warn():
    healthy = _status(partition=0)
    degraded = _status(partition=1, replicas=(1, 2, 3), isr=(1, 2))
    summary = summarize_replication([healthy, degraded])
    assert summary.severity == HealthSeverity.WARN
    assert summary.under_replicated_partitions == [degraded]
    assert "orders-1" in summary.message


def test_offline_replica_is_critical_even_alongside_under_replicated():
    under_replicated = _status(partition=0, replicas=(1, 2, 3), isr=(1, 2))
    offline = _status(partition=1, replicas=(1, 2, 3), isr=(1,), offline=(2, 3))
    summary = summarize_replication([under_replicated, offline])
    assert summary.severity == HealthSeverity.CRITICAL
    assert summary.offline_partitions == [offline]
    # An offline partition is also under-replicated, but it's reported once,
    # under the more severe bucket, not double-counted in both.
    assert offline not in summary.under_replicated_partitions


def test_partition_status_helpers():
    healthy = _status()
    assert not healthy.is_under_replicated
    assert not healthy.is_offline

    degraded = _status(replicas=(1, 2, 3), isr=(1, 2))
    assert degraded.is_under_replicated
    assert not degraded.is_offline

    offline = _status(replicas=(1, 2, 3), isr=(1,), offline=(2,))
    assert offline.is_under_replicated
    assert offline.is_offline


def test_message_names_up_to_five_partitions_then_summarizes_the_rest():
    degraded = [_status(partition=i, replicas=(1, 2), isr=(1,)) for i in range(7)]
    summary = summarize_replication(degraded)
    assert "orders-0" in summary.message
    assert "orders-4" in summary.message
    assert "orders-5" not in summary.message
    assert "2 more" in summary.message
