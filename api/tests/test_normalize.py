"""Feeds real adapter dataclasses into the normalizers — no live broker
needed, since these are just plain Python objects, but it does prove the
normalize.py functions actually match each adapter's real shapes (not
just a hand-rolled test double that could drift from the real thing).
"""

from kafka_adapter.models import ConsumerGroup as KafkaConsumerGroup
from kafka_adapter.models import HealthCategory as KafkaHealthCategory
from kafka_adapter.models import HealthEvent as KafkaHealthEvent
from kafka_adapter.models import HealthSeverity as KafkaHealthSeverity
from kafka_adapter.models import MessageSample as KafkaMessageSample
from kafka_adapter.models import PartitionLag
from kafka_adapter.models import Resource as KafkaResource
from mq_adapter.models import HealthCategory as MQHealthCategory
from mq_adapter.models import HealthEvent as MQHealthEvent
from mq_adapter.models import HealthSeverity as MQHealthSeverity
from mq_adapter.models import Resource as MQResource
from solace_adapter.models import HealthCategory as SolaceHealthCategory
from solace_adapter.models import HealthEvent as SolaceHealthEvent
from solace_adapter.models import HealthSeverity as SolaceHealthSeverity
from solace_adapter.models import Resource as SolaceResource

from api import normalize


def test_kafka_resource_normalizes_with_system_type_tag():
    r = KafkaResource(id="b1:orders", broker_id="b1", namespace="localhost:9092", name="orders", kind="topic", consumer_lag=20, partition_count=3)
    unified = normalize.kafka_resource(r, "b1")
    assert unified.system_type == "kafka"
    assert unified.consumer_lag == 20
    assert unified.depth_current is None  # Kafka has no depth concept


def test_kafka_health_event_unwraps_enum_to_plain_string():
    h = KafkaHealthEvent(broker_id="b1", severity=KafkaHealthSeverity.WARN, category=KafkaHealthCategory.REPLICATION, message="uh oh", timestamp="t")
    unified = normalize.kafka_health_event(h, "b1")
    assert unified.severity == "warn"
    assert unified.category == "replication"
    assert unified.system_type == "kafka"


def test_kafka_consumer_group_normalizes_partitions():
    g = KafkaConsumerGroup(
        id="b1:g1", broker_id="b1", group_name="g1", state="Stable", member_count=1,
        partitions=[PartitionLag(topic="orders", partition=0, log_end_offset=10, committed_offset=5, lag=5)],
    )
    unified = normalize.kafka_consumer_group(g, "b1")
    assert unified.group_name == "g1"
    assert len(unified.partitions) == 1
    assert unified.partitions[0].lag == 5


def test_kafka_message_sample_combines_partition_and_offset_into_message_id():
    m = KafkaMessageSample(resource_id="b1:orders", partition=2, offset=7, timestamp=123, headers={}, body_preview="hi", size_bytes=2)
    unified = normalize.kafka_message_sample(m)
    assert unified.message_id == "2:7"


def test_solace_resource_normalizes_with_system_type_tag():
    r = SolaceResource(id="b2:default:q1", broker_id="b2", namespace="default", name="q1", kind="queue", depth_current=4, depth_max=1000)
    unified = normalize.solace_resource(r, "b2")
    assert unified.system_type == "solace"
    assert unified.namespace == "default"
    assert unified.consumer_lag is None  # no per-consumer offset model


def test_solace_health_event_unwraps_enum():
    h = SolaceHealthEvent(broker_id="b2", severity=SolaceHealthSeverity.CRITICAL, category=SolaceHealthCategory.SPOOL, message="full", timestamp="t")
    unified = normalize.solace_health_event(h, "b2")
    assert unified.severity == "critical"
    assert unified.system_type == "solace"


def test_mq_resource_normalizes_with_system_type_tag():
    r = MQResource(id="b3:QM1:Q1", broker_id="b3", namespace="QM1", name="Q1", kind="queue", depth_current=3, depth_max=5000)
    unified = normalize.mq_resource(r, "b3")
    assert unified.system_type == "mq"
    assert unified.depth_current == 3
    assert unified.consumer_lag is None  # no per-consumer offset model in MQ


def test_mq_health_event_unwraps_enum():
    h = MQHealthEvent(broker_id="b3", severity=MQHealthSeverity.OK, category=MQHealthCategory.CONNECTIVITY, message="running", timestamp="t")
    unified = normalize.mq_health_event(h, "b3")
    assert unified.severity == "ok"
    assert unified.system_type == "mq"


def test_all_three_resource_types_share_the_same_unified_shape():
    # The whole point of this layer: once normalized, nothing downstream
    # needs a system_type-specific code path to read basic fields.
    kafka = normalize.kafka_resource(
        KafkaResource(id="k", broker_id="b1", namespace="n", name="orders", kind="topic"), "b1"
    )
    solace = normalize.solace_resource(
        SolaceResource(id="s", broker_id="b2", namespace="n", name="q1", kind="queue"), "b2"
    )
    mq = normalize.mq_resource(MQResource(id="m", broker_id="b3", namespace="n", name="Q1", kind="queue"), "b3")

    for resource in (kafka, solace, mq):
        assert resource.model_dump().keys() == kafka.model_dump().keys()
