from unittest.mock import patch

from api.aggregator import BrokerRegistry
from api.config import BrokerConfig


def _configs():
    return [
        BrokerConfig(id="b1", type="kafka", name="B1", environment="dev", config={"bootstrap_servers": "x"}),
        BrokerConfig(id="b2", type="solace", name="B2", environment="dev", config={}),
    ]


def test_fetch_all_marks_unreachable_broker_down_without_failing_the_rest():
    registry = BrokerRegistry(_configs())
    with patch.object(BrokerRegistry, "_fetch_kafka", side_effect=ConnectionError("no route")):
        with patch.object(BrokerRegistry, "_fetch_solace", return_value=([], [], [])):
            brokers, resources, health, groups = registry.fetch_all()

    b1 = next(b for b in brokers if b.id == "b1")
    b2 = next(b for b in brokers if b.id == "b2")
    assert b1.status == "down"
    assert b2.status == "up"
    # the broken broker still produces a visible connectivity HealthEvent
    down_events = [h for h in health if h.broker_id == "b1" and h.category == "connectivity"]
    assert len(down_events) == 1
    assert down_events[0].severity == "critical"
    assert "no route" in down_events[0].message


def test_fetch_all_returns_data_for_healthy_brokers_normally():
    registry = BrokerRegistry(_configs())
    with patch.object(BrokerRegistry, "_fetch_kafka", return_value=([], [], [])):
        with patch.object(BrokerRegistry, "_fetch_solace", return_value=([], [], [])):
            brokers, _, _, _ = registry.fetch_all()

    assert all(b.status == "up" for b in brokers)


def test_get_config_looks_up_by_broker_id():
    registry = BrokerRegistry(_configs())
    assert registry.get_config("b1").type == "kafka"
    assert registry.get_config("missing") is None


def test_peek_raises_for_unknown_broker():
    from api.models import Resource

    registry = BrokerRegistry(_configs())
    resource = Resource(id="x", broker_id="does-not-exist", system_type="kafka", namespace="n", name="n", kind="topic")
    try:
        registry.peek(resource)
        assert False, "expected KeyError"
    except KeyError:
        pass


def test_add_broker_appends_and_is_findable():
    registry = BrokerRegistry([])
    new = BrokerConfig(id="new1", type="kafka", name="New", environment="dev", config={"bootstrap_servers": "x"})
    registry.add_broker(new)
    assert registry.get_config("new1") is new
    assert new in registry.broker_configs


def test_add_broker_rejects_duplicate_id():
    registry = BrokerRegistry(_configs())
    dup = BrokerConfig(id="b1", type="solace", name="Dup", environment="dev", config={})
    try:
        registry.add_broker(dup)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_remove_broker_drops_it_from_both_the_list_and_lookup():
    registry = BrokerRegistry(_configs())
    registry.remove_broker("b1")
    assert registry.get_config("b1") is None
    assert all(b.id != "b1" for b in registry.broker_configs)


def test_remove_broker_raises_for_unknown_id():
    registry = BrokerRegistry(_configs())
    try:
        registry.remove_broker("nope")
        assert False, "expected KeyError"
    except KeyError:
        pass


def test_test_connection_returns_none_on_success():
    registry = BrokerRegistry([])
    bc = BrokerConfig(id="b1", type="kafka", name="B1", environment="dev", config={"bootstrap_servers": "x"})
    with patch.object(BrokerRegistry, "_fetch_kafka", return_value=([], [], [])):
        assert registry.test_connection(bc) is None


def test_test_connection_returns_error_message_on_failure():
    registry = BrokerRegistry([])
    bc = BrokerConfig(id="b1", type="kafka", name="B1", environment="dev", config={"bootstrap_servers": "x"})
    with patch.object(BrokerRegistry, "_fetch_kafka", side_effect=ConnectionError("nope")):
        error = registry.test_connection(bc)
    assert error is not None
    assert "nope" in error


# -- peek() credential override -----------------------------------------
# The point: a peek retry with different credentials must never need (or
# touch) whatever's saved in the broker config — proven here by giving
# Solace/MQ configs that are missing username/password entirely and
# confirming the override alone is what gets used.


def test_peek_solace_uses_override_credentials_not_saved_config():
    from api.models import Resource

    bc = BrokerConfig(
        id="b2", type="solace", name="B2", environment="dev",
        config={"smf_host": "tcp://x:55555", "username": "saved-user", "password": "saved-pass"},
    )
    registry = BrokerRegistry([bc])
    resource = Resource(id="b2:testVPN:q1", broker_id="b2", system_type="solace", namespace="testVPN", name="q1", kind="queue")

    with patch("solace_adapter.peek.peek_messages", return_value=[]) as mock_peek:
        registry.peek(resource, limit=5, override_username="override-user", override_password="override-pass")

    assert mock_peek.call_args.kwargs["username"] == "override-user"
    assert mock_peek.call_args.kwargs["password"] == "override-pass"
    assert mock_peek.call_args.kwargs["vpn_name"] == "testVPN"


def test_peek_solace_falls_back_to_saved_config_without_override():
    from api.models import Resource

    bc = BrokerConfig(
        id="b2", type="solace", name="B2", environment="dev",
        config={"smf_host": "tcp://x:55555", "username": "saved-user", "password": "saved-pass"},
    )
    registry = BrokerRegistry([bc])
    resource = Resource(id="b2:default:q1", broker_id="b2", system_type="solace", namespace="default", name="q1", kind="queue")

    with patch("solace_adapter.peek.peek_messages", return_value=[]) as mock_peek:
        registry.peek(resource, limit=5)

    assert mock_peek.call_args.kwargs["username"] == "saved-user"
    assert mock_peek.call_args.kwargs["password"] == "saved-pass"


def test_peek_mq_uses_override_credentials_not_saved_config():
    from api.models import Resource

    bc = BrokerConfig(
        id="b3", type="mq", name="B3", environment="dev",
        config={"admin_url": "https://x", "qmgr_name": "QM1", "app_username": "saved-user", "app_password": "saved-pass"},
    )
    registry = BrokerRegistry([bc])
    resource = Resource(id="b3:QM1:Q1", broker_id="b3", system_type="mq", namespace="QM1", name="Q1", kind="queue")

    with patch("mq_adapter.peek.peek_messages", return_value=[]) as mock_peek:
        registry.peek(resource, limit=5, override_username="override-user", override_password="override-pass")

    # session.auth was set from the override before being passed in
    used_session = mock_peek.call_args.kwargs["session"]
    assert used_session.auth == ("override-user", "override-pass")


def test_peek_kafka_uses_override_sasl_credentials_not_saved_config():
    from api.models import Resource

    # No sasl_username in the saved config at all — proves the override
    # doesn't need one already there to take effect (a PLAINTEXT cluster
    # being retried with SASL credentials for a topic that turns out to
    # need them, say).
    bc = BrokerConfig(id="b1", type="kafka", name="B1", environment="dev", config={"bootstrap_servers": "x:9092"})
    registry = BrokerRegistry([bc])
    resource = Resource(id="b1:orders", broker_id="b1", system_type="kafka", namespace="x:9092", name="orders", kind="topic")

    with patch("kafka_adapter.peek.peek_messages", return_value=[]) as mock_peek:
        registry.peek(resource, limit=5, override_username="override-user", override_password="override-pass")

    assert mock_peek.call_args.kwargs["sasl_plain_username"] == "override-user"
    assert mock_peek.call_args.kwargs["sasl_plain_password"] == "override-pass"
    assert mock_peek.call_args.kwargs["sasl_mechanism"] == "PLAIN"  # defaulted since the config had none configured


def test_peek_kafka_falls_back_to_saved_config_without_override():
    from api.models import Resource

    bc = BrokerConfig(id="b1", type="kafka", name="B1", environment="dev", config={"bootstrap_servers": "x:9092"})
    registry = BrokerRegistry([bc])
    resource = Resource(id="b1:orders", broker_id="b1", system_type="kafka", namespace="x:9092", name="orders", kind="topic")

    with patch("kafka_adapter.peek.peek_messages", return_value=[]) as mock_peek:
        registry.peek(resource, limit=5)

    assert mock_peek.call_args.kwargs["sasl_plain_username"] is None
    assert mock_peek.call_args.kwargs["security_protocol"] == "PLAINTEXT"
