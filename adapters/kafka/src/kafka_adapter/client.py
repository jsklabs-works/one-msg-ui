"""Talks to a Kafka cluster and normalizes what it finds into the shared
schema (see models.py / /docs/architecture.md §3).

Primary source: the Kafka Admin API, via kafka-python's KafkaAdminClient
and KafkaConsumer (used here for topic/offset discovery, which the admin
client doesn't expose as directly). Replication/ISR health comes from the
Admin API's topic metadata too (see replication.py) — JMX turned out not
to be necessary for that after all, only for broker-resource metrics this
adapter doesn't cover yet (request latency percentiles, handler idle
ratio, etc.).
"""

from __future__ import annotations

import datetime as _dt

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.structs import TopicPartition

from .lag import compute_lag, sum_topic_lag
from .models import ConsumerGroup, HealthCategory, HealthEvent, HealthSeverity, PartitionLag, Resource
from .replication import PartitionReplicationStatus, summarize_replication

_INTERNAL_TOPIC_PREFIX = "__"


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def security_kwargs(
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: str | None = None,
    sasl_plain_username: str | None = None,
    sasl_plain_password: str | None = None,
    ssl_cafile: str | None = None,
) -> dict:
    """kwargs shared by every KafkaConsumer/KafkaAdminClient this adapter
    constructs, so TLS/SASL config is set in exactly one place. Kept as a
    plain function (not baked into __init__) so client.py and peek.py's
    scratch consumer use identical settings — verified real kwargs against
    kafka-python-ng's KafkaConsumer.DEFAULT_CONFIG, not guessed.
    """
    kwargs: dict = {"security_protocol": security_protocol}
    if sasl_mechanism:
        kwargs["sasl_mechanism"] = sasl_mechanism
        kwargs["sasl_plain_username"] = sasl_plain_username
        kwargs["sasl_plain_password"] = sasl_plain_password
    if ssl_cafile:
        kwargs["ssl_cafile"] = ssl_cafile
    return kwargs


class KafkaAdapter:
    """One instance per broker/cluster, per the Broker shape in the shared model."""

    def __init__(
        self,
        broker_id: str,
        bootstrap_servers: str,
        namespace: str | None = None,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str | None = None,
        sasl_plain_username: str | None = None,
        sasl_plain_password: str | None = None,
        ssl_cafile: str | None = None,
    ):
        self.broker_id = broker_id
        self.bootstrap_servers = bootstrap_servers
        self.namespace = namespace or bootstrap_servers
        sec_kwargs = security_kwargs(security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password, ssl_cafile)
        self._admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="one-msg-ui-kafka-adapter", **sec_kwargs)
        # A plain consumer (no group) is the simplest way to discover
        # topics/partitions and their end offsets — the admin client itself
        # doesn't expose end-offset lookups.
        self._consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            client_id="one-msg-ui-kafka-adapter-discovery",
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            **sec_kwargs,
        )

    def close(self) -> None:
        self._admin.close()
        self._consumer.close()

    # -- topics / resources --------------------------------------------

    def list_topic_names(self) -> list[str]:
        return sorted(t for t in self._consumer.topics() if not t.startswith(_INTERNAL_TOPIC_PREFIX))

    def _end_offsets_for_topic(self, topic: str) -> dict[int, int]:
        partitions = self._consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in partitions]
        if not tps:
            return {}
        offsets = self._consumer.end_offsets(tps)
        return {tp.partition: offset for tp, offset in offsets.items()}

    def get_resources(self, consumer_groups: list[ConsumerGroup] | None = None) -> list[Resource]:
        """One Resource per topic. consumer_lag is the sum of lag across all
        consumer groups' partitions for that topic — None if no group has
        consumed it (distinct from a genuinely-zero lag), per the adapter
        README's "no lag data" vs. "zero lag" note.
        """
        groups = consumer_groups if consumer_groups is not None else self.get_consumer_groups()

        lags_by_topic: dict[str, list[int | None]] = {}
        for group in groups:
            for p in group.partitions:
                lags_by_topic.setdefault(p.topic, []).append(p.lag)

        resources = []
        for topic in self.list_topic_names():
            end_offsets = self._end_offsets_for_topic(topic)
            resources.append(
                Resource(
                    id=f"{self.broker_id}:{topic}",
                    broker_id=self.broker_id,
                    namespace=self.namespace,
                    name=topic,
                    kind="topic",
                    depth_current=None,
                    depth_max=None,
                    consumer_lag=sum_topic_lag(lags_by_topic.get(topic, [])),
                    partition_count=len(end_offsets),
                    last_updated=_now_iso(),
                )
            )
        return resources

    # -- consumer groups / lag -------------------------------------------

    def get_consumer_groups(self) -> list[ConsumerGroup]:
        groups_out: list[ConsumerGroup] = []
        raw_groups = self._admin.list_consumer_groups()  # list[(group_id, protocol_type)]
        group_ids = [g[0] for g in raw_groups if g[0]]
        if not group_ids:
            return groups_out

        descriptions = {d.group: d for d in self._admin.describe_consumer_groups(group_ids)}

        for group_id in group_ids:
            desc = descriptions.get(group_id)
            state = desc.state if desc else "unknown"
            member_count = len(desc.members) if desc else 0

            committed = self._admin.list_consumer_group_offsets(group_id)  # {TopicPartition: OffsetAndMetadata}
            partitions: list[PartitionLag] = []
            for tp, offset_meta in committed.items():
                if tp.topic.startswith(_INTERNAL_TOPIC_PREFIX):
                    continue
                end_offsets = self._end_offsets_for_topic(tp.topic)
                log_end_offset = end_offsets.get(tp.partition)
                committed_offset = offset_meta.offset if offset_meta.offset is not None and offset_meta.offset >= 0 else None
                lag = compute_lag(log_end_offset, committed_offset)
                partitions.append(
                    PartitionLag(
                        topic=tp.topic,
                        partition=tp.partition,
                        log_end_offset=log_end_offset if log_end_offset is not None else -1,
                        committed_offset=committed_offset,
                        lag=lag,
                    )
                )

            groups_out.append(
                ConsumerGroup(
                    id=f"{self.broker_id}:{group_id}",
                    broker_id=self.broker_id,
                    group_name=group_id,
                    state=state,
                    member_count=member_count,
                    partitions=partitions,
                )
            )
        return groups_out

    # -- health -----------------------------------------------------------

    def _connectivity_health_event(self) -> HealthEvent:
        try:
            brokers = self._admin._client.cluster.brokers()  # best-effort; not public API
            broker_count = len(list(brokers)) if brokers else 0
            severity = HealthSeverity.OK if broker_count > 0 else HealthSeverity.CRITICAL
            message = f"{broker_count} broker(s) visible in cluster metadata"
        except Exception as exc:  # pragma: no cover - defensive, cluster internals vary by version
            severity = HealthSeverity.WARN
            message = f"could not read cluster metadata: {exc}"

        return HealthEvent(
            broker_id=self.broker_id,
            severity=severity,
            category=HealthCategory.CONNECTIVITY,
            message=message,
            timestamp=_now_iso(),
        )

    def _replication_statuses(self) -> list[PartitionReplicationStatus]:
        """Per-partition replicas/isr/offline_replicas straight from the
        Admin API's topic metadata (no JMX needed — see replication.py).
        """
        topics_metadata = self._admin.describe_topics(topics=None)
        statuses: list[PartitionReplicationStatus] = []
        for topic_meta in topics_metadata:
            topic = topic_meta.get("topic")
            if not topic or topic.startswith(_INTERNAL_TOPIC_PREFIX):
                continue
            for p in topic_meta.get("partitions", []):
                statuses.append(
                    PartitionReplicationStatus(
                        topic=topic,
                        partition=p["partition"],
                        replicas=list(p.get("replicas", [])),
                        isr=list(p.get("isr", [])),
                        offline_replicas=list(p.get("offline_replicas", [])),
                    )
                )
        return statuses

    def _replication_health_event(self) -> HealthEvent | None:
        statuses = self._replication_statuses()
        if not statuses:
            return None
        summary = summarize_replication(statuses)
        return HealthEvent(
            broker_id=self.broker_id,
            severity=summary.severity,
            category=HealthCategory.REPLICATION,
            message=summary.message,
            timestamp=_now_iso(),
        )

    def get_health_events(self) -> list[HealthEvent]:
        events = [self._connectivity_health_event()]
        replication_event = self._replication_health_event()
        if replication_event is not None:
            events.append(replication_event)
        return events
