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
from .config import BrokerConfig, config_bool, connection_identity

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _kafka_security_kwargs(config: dict) -> dict:
    """Turns the add-broker form's use_tls/sasl_*/ssl_cafile fields into
    the kwargs kafka_adapter.client.security_kwargs expects. Kept here
    (not in kafka_adapter) since it's specifically about translating this
    API's config-dict shape, not a Kafka-protocol concern.
    """
    use_tls = config_bool(config, "use_tls", False)
    sasl_username = config.get("sasl_username") or None
    sasl_password = config.get("sasl_password") or None
    if sasl_username:
        security_protocol = "SASL_SSL" if use_tls else "SASL_PLAINTEXT"
    else:
        security_protocol = "SSL" if use_tls else "PLAINTEXT"
    return {
        "security_protocol": security_protocol,
        "sasl_mechanism": "PLAIN" if sasl_username else None,
        "sasl_plain_username": sasl_username,
        "sasl_plain_password": sasl_password,
        "ssl_cafile": config.get("ssl_cafile") or None,
    }


class BrokerRegistry:
    def __init__(self, broker_configs: list[BrokerConfig]):
        self.broker_configs = broker_configs
        self._by_id = {b.id: b for b in broker_configs}

    def get_config(self, broker_id: str) -> BrokerConfig | None:
        return self._by_id.get(broker_id)

    def find_duplicate(self, system_type: str, config: dict) -> BrokerConfig | None:
        """An existing broker already pointing at the same physical
        endpoint (see config.connection_identity) — checked before adding
        a new one so two different display names can't both connect to
        the same broker. A config with no identifying fields set at all
        (e.g. still mid-validation-error) never matches anything, rather
        than every such config colliding with every other.
        """
        identity = connection_identity(system_type, config)
        if not any(identity):
            return None
        for bc in self.broker_configs:
            if bc.type == system_type and connection_identity(bc.type, bc.config) == identity:
                return bc
        return None

    def add_broker(self, bc: BrokerConfig) -> None:
        if bc.id in self._by_id:
            raise ValueError(f"broker id already exists: {bc.id}")
        self.broker_configs.append(bc)
        self._by_id[bc.id] = bc

    def remove_broker(self, broker_id: str) -> None:
        if broker_id not in self._by_id:
            raise KeyError(f"no such broker: {broker_id}")
        self.broker_configs = [b for b in self.broker_configs if b.id != broker_id]
        del self._by_id[broker_id]

    def test_connection(self, bc: BrokerConfig) -> str | None:
        """Try to actually reach the broker before it's saved — an "add
        broker" form that only fails silently later (on the next poll) is
        worse than one that tells you now the SEMP URL is wrong. Returns
        None on success, or an error message.
        """
        fetcher = getattr(self, self._FETCHER_NAMES[bc.type])
        try:
            fetcher(bc)
            return None
        except Exception as exc:
            return str(exc)

    # -- per-broker-type fetchers, each isolated so one broker's failure
    #    can't take down another's data -----------------------------------

    def _fetch_kafka(self, bc: BrokerConfig):
        from kafka_adapter.client import KafkaAdapter

        adapter = KafkaAdapter(broker_id=bc.id, bootstrap_servers=bc.config["bootstrap_servers"], **_kafka_security_kwargs(bc.config))
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
            # Optional filter, not a required scope — unset means "every VPN
            # these credentials can see" (see SolaceAdapter's docstring for
            # why a broker connection was never really "one VPN").
            vpn_name=bc.config.get("vpn_name") or None,
            username=bc.config["username"],
            password=bc.config["password"],
            verify_certificate=config_bool(bc.config, "verify_certificate", True),
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
            verify_tls=config_bool(bc.config, "verify_tls", False),
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

    def peek(
        self,
        resource: models.Resource,
        limit: int = 10,
        override_username: str | None = None,
        override_password: str | None = None,
    ) -> list[models.MessageSample]:
        """override_username/password let a caller retry with different
        credentials for just this call, without touching the saved broker
        config — the point being Solace in particular: SEMP admin
        credentials are broker-wide, but an SMF connection for peek
        authenticates per VPN, and a VPN can genuinely need its own
        username/password that the broker-level config doesn't have (see
        adapters/solace/README.md). Neither override is persisted; the UI
        prompts for them fresh each time a peek fails on auth.
        """
        bc = self.get_config(resource.broker_id)
        if bc is None:
            raise KeyError(f"no such broker: {resource.broker_id}")

        if bc.type == "kafka":
            from kafka_adapter.peek import peek_messages

            security_kwargs = _kafka_security_kwargs(bc.config)
            if override_username:
                security_kwargs["sasl_mechanism"] = security_kwargs["sasl_mechanism"] or "PLAIN"
                security_kwargs["sasl_plain_username"] = override_username
                security_kwargs["sasl_plain_password"] = override_password
            samples = peek_messages(
                bootstrap_servers=bc.config["bootstrap_servers"],
                resource_id=resource.id,
                topic=resource.name,
                limit=limit,
                **security_kwargs,
            )
            return [normalize.kafka_message_sample(m) for m in samples]

        if bc.type == "solace":
            from solace_adapter.peek import peek_messages

            samples = peek_messages(
                smf_host=bc.config["smf_host"],
                # The resource's own VPN, not the broker config's (possibly
                # unset, "all VPNs") filter — an SMF connection is always
                # scoped to exactly one VPN, and `resource.namespace` always
                # names the right one regardless of how many VPNs this
                # broker connection spans.
                vpn_name=resource.namespace,
                username=override_username or bc.config["username"],
                password=override_password if override_username else bc.config["password"],
                resource_id=resource.id,
                queue_name=resource.name,
                limit=limit,
                verify_certificate=config_bool(bc.config, "verify_certificate", True),
            )
            return [normalize.solace_message_sample(m) for m in samples]

        if bc.type == "mq":
            import requests

            from mq_adapter.peek import peek_messages

            session = requests.Session()
            session.auth = (override_username or bc.config["app_username"], override_password if override_username else bc.config["app_password"])
            session.verify = config_bool(bc.config, "verify_tls", False)
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
