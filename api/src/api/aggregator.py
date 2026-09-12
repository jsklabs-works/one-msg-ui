"""Fans out to each configured broker's adapter and normalizes the
results. This is a live passthrough for MVP purposes — every API request
queries the brokers directly. It is deliberately NOT the
ingestion/poller + time-series store pipeline described in
architecture.md §4; that's still the right design for anything beyond a
small number of brokers/local dev, since polling every broker on every
UI request doesn't scale and adds needless load. See api/README.md.

A broker that can't be reached is reported as status="down" with a
connectivity HealthEvent, rather than making the whole request fail —
an on-call view that goes blank because one broker is unreachable is
worse than one that shows two-out-of-three brokers plus a clear error.
"""

from __future__ import annotations

import datetime as _dt
import logging

from . import models, normalize
from .config import BrokerConfig

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


class BrokerRegistry:
    def __init__(self, broker_configs: list[BrokerConfig]):
        self.broker_configs = broker_configs
        self._by_id = {b.id: b for b in broker_configs}

    def get_config(self, broker_id: str) -> BrokerConfig | None:
        return self._by_id.get(broker_id)

    # -- per-broker-type fetchers, each isolated so one broker's failure
    #    can't take down another's data -----------------------------------

    def _fetch_kafka(self, bc: BrokerConfig):
        from kafka_adapter.client import KafkaAdapter

        adapter = KafkaAdapter(broker_id=bc.id, bootstrap_servers=bc.config["bootstrap_servers"])
        try:
            groups = adapter.get_consumer_groups()
            resources = [normalize.kafka_resource(r, bc.id) for r in adapter.get_resources(consumer_groups=groups)]
            health = [normalize.kafka_health_event(h, bc.id) for h in adapter.get_health_events()]
            consumer_groups = [normalize.kafka_consumer_group(g, bc.id) for g in groups]
            return resources, health, consumer_groups
        finally:
            adapter.close()

    def _fetch_solace(self, bc: BrokerConfig):
        from solace_adapter.client import SolaceAdapter

        adapter = SolaceAdapter(
            broker_id=bc.id,
            base_url=bc.config["semp_url"],
            vpn_name=bc.config["vpn_name"],
            username=bc.config["username"],
            password=bc.config["password"],
        )
        try:
            resources = [normalize.solace_resource(r, bc.id) for r in adapter.get_resources()]
            health = [normalize.solace_health_event(h, bc.id) for h in adapter.get_health_events()]
            return resources, health, []
        finally:
            adapter.close()

    def _fetch_mq(self, bc: BrokerConfig):
        from mq_adapter.client import MQAdapter

        adapter = MQAdapter(
            broker_id=bc.id,
            base_url=bc.config["admin_url"],
            qmgr_name=bc.config["qmgr_name"],
            username=bc.config["admin_username"],
            password=bc.config["admin_password"],
        )
        try:
            raw_resources = adapter.get_resources(queue_pattern=bc.config.get("queue_pattern", "*"))
            resources = [normalize.mq_resource(r, bc.id) for r in raw_resources]
            health = [normalize.mq_health_event(h, bc.id) for h in adapter.get_health_events(resources=raw_resources)]
            return resources, health, []
        finally:
            adapter.close()

    # Method *names*, looked up via getattr at call time — not the bound
    # methods themselves, so that patching a method on the class (as the
    # tests do) actually takes effect.
    _FETCHER_NAMES = {"kafka": "_fetch_kafka", "solace": "_fetch_solace", "mq": "_fetch_mq"}

    def fetch_all(self) -> tuple[list[models.Broker], list[models.Resource], list[models.HealthEvent], list[models.ConsumerGroup]]:
        brokers: list[models.Broker] = []
        all_resources: list[models.Resource] = []
        all_health: list[models.HealthEvent] = []
        all_groups: list[models.ConsumerGroup] = []

        for bc in self.broker_configs:
            fetcher = getattr(self, self._FETCHER_NAMES[bc.type])
            try:
                resources, health, groups = fetcher(bc)
                brokers.append(models.Broker(id=bc.id, system_type=bc.type, name=bc.name, environment=bc.environment, status="up"))
                all_resources.extend(resources)
                all_health.extend(health)
                all_groups.extend(groups)
            except Exception as exc:
                logger.warning("broker %s (%s) unreachable: %s", bc.id, bc.type, exc)
                brokers.append(models.Broker(id=bc.id, system_type=bc.type, name=bc.name, environment=bc.environment, status="down"))
                all_health.append(
                    models.HealthEvent(
                        broker_id=bc.id,
                        system_type=bc.type,
                        severity="critical",
                        category="connectivity",
                        message=f"broker unreachable: {exc}",
                        timestamp=_now_iso(),
                    )
                )

        return brokers, all_resources, all_health, all_groups

    def peek(self, resource: models.Resource, limit: int = 10) -> list[models.MessageSample]:
        bc = self.get_config(resource.broker_id)
        if bc is None:
            raise KeyError(f"no such broker: {resource.broker_id}")

        if bc.type == "kafka":
            from kafka_adapter.peek import peek_messages

            samples = peek_messages(
                bootstrap_servers=bc.config["bootstrap_servers"],
                resource_id=resource.id,
                topic=resource.name,
                limit=limit,
            )
            return [normalize.kafka_message_sample(m) for m in samples]

        if bc.type == "solace":
            from solace_adapter.peek import peek_messages

            samples = peek_messages(
                smf_host=bc.config["smf_host"],
                vpn_name=bc.config["vpn_name"],
                username=bc.config["username"],
                password=bc.config["password"],
                resource_id=resource.id,
                queue_name=resource.name,
                limit=limit,
            )
            return [normalize.solace_message_sample(m) for m in samples]

        if bc.type == "mq":
            import requests

            from mq_adapter.peek import peek_messages

            session = requests.Session()
            session.auth = (bc.config["app_username"], bc.config["app_password"])
            session.verify = False
            try:
                samples = peek_messages(
                    session=session,
                    base_url=bc.config["admin_url"],
                    qmgr_name=bc.config["qmgr_name"],
                    queue_name=resource.name,
                    resource_id=resource.id,
                    limit=limit,
                )
            finally:
                session.close()
            return [normalize.mq_message_sample(m) for m in samples]

        raise ValueError(f"unknown broker type: {bc.type}")
