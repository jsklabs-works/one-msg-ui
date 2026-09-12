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
