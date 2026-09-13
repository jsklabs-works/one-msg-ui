from unittest.mock import patch

from activemq_adapter.client import ActiveMQAdapter


def _adapter():
    return ActiveMQAdapter(broker_id="b1", base_url="http://example:8161", username="u", password="p")


def test_search_and_read_delegate_to_post():
    adapter = _adapter()
    with patch.object(adapter, "_post", return_value=["mbean1"]) as mock_post:
        result = adapter._search("some:pattern=*")
    assert result == ["mbean1"]
    mock_post.assert_called_once_with({"type": "search", "mbean": "some:pattern=*"})


def test_broker_mbean_name_is_discovered_once_and_cached():
    adapter = _adapter()
    with patch.object(adapter, "_search", return_value=['org.apache.activemq.artemis:broker="0.0.0.0"']) as mock_search:
        name1 = adapter._broker_mbean_name()
        name2 = adapter._broker_mbean_name()
    assert name1 == name2 == 'org.apache.activemq.artemis:broker="0.0.0.0"'
    mock_search.assert_called_once()  # second call used the cache, no second search


def test_broker_mbean_name_raises_when_none_found():
    adapter = _adapter()
    with patch.object(adapter, "_search", return_value=[]):
        try:
            adapter._broker_mbean_name()
            assert False, "expected RuntimeError"
        except RuntimeError:
            pass


def test_get_resources_maps_jolokia_attributes_to_resource_shape():
    adapter = _adapter()
    queue_mbean = 'org.apache.activemq.artemis:address="orders",broker="0.0.0.0",component=addresses,queue="orders",routing-type="anycast",subcomponent=queues'
    attrs = {"Address": "orders", "Name": "orders", "MessageCount": 4, "ConsumerCount": 2, "RingSize": -1}
    with patch.object(adapter, "_search", return_value=[queue_mbean]) as mock_search:
        with patch.object(adapter, "_read", return_value=attrs):
            resources = adapter.get_resources()

    mock_search.assert_called_once_with("org.apache.activemq.artemis:component=addresses,subcomponent=queues,*")
    assert len(resources) == 1
    r = resources[0]
    assert r.id == "b1:orders:orders"
    assert r.namespace == "orders"
    assert r.name == "orders"
    assert r.kind == "queue"
    assert r.depth_current == 4
    assert r.depth_max is None  # RingSize -1 means unbounded
    assert r.consumer_count == 2
    assert r.consumer_lag is None


def test_get_resources_reports_ring_size_as_depth_max_when_set():
    adapter = _adapter()
    attrs = {"Address": "bounded", "Name": "bounded", "MessageCount": 5, "RingSize": 100}
    with patch.object(adapter, "_search", return_value=["mbean"]):
        with patch.object(adapter, "_read", return_value=attrs):
            resources = adapter.get_resources()
    assert resources[0].depth_max == 100


def test_get_resources_spans_every_discovered_queue_mbean():
    adapter = _adapter()
    attrs_by_mbean = {
        "q1-mbean": {"Address": "addr1", "Name": "q1", "MessageCount": 1},
        "q2-mbean": {"Address": "addr2", "Name": "q2", "MessageCount": 2},
    }
    with patch.object(adapter, "_search", return_value=list(attrs_by_mbean.keys())):
        with patch.object(adapter, "_read", side_effect=lambda mbean: attrs_by_mbean[mbean]):
            resources = adapter.get_resources()
    assert {(r.namespace, r.name) for r in resources} == {("addr1", "q1"), ("addr2", "q2")}


def test_get_health_events_covers_broker_state_and_memory_and_appends_depth_event():
    adapter = _adapter()
    broker_attrs = {"Name": "0.0.0.0", "Started": True, "Active": True, "AddressMemoryUsage": 0, "GlobalMaxSize": 1000}
    with patch.object(adapter, "_broker_mbean_name", return_value="broker-mbean"):
        with patch.object(adapter, "_read", return_value=broker_attrs):
            events = adapter.get_health_events(resources=[])

    connectivity = [e for e in events if e.category == "connectivity"]
    capacity = [e for e in events if e.category == "capacity"]
    assert len(connectivity) == 1
    assert connectivity[0].severity == "ok"
    # two capacity events: broker memory usage + queue-depth summary
    assert len(capacity) == 2
    assert any("no queue ring-size limits" in e.message for e in capacity)


def test_get_health_events_flags_a_stopped_broker_as_critical():
    adapter = _adapter()
    broker_attrs = {"Name": "0.0.0.0", "Started": False, "Active": False, "AddressMemoryUsage": 0, "GlobalMaxSize": 0}
    with patch.object(adapter, "_broker_mbean_name", return_value="broker-mbean"):
        with patch.object(adapter, "_read", return_value=broker_attrs):
            events = adapter.get_health_events(resources=[])
    connectivity = next(e for e in events if e.category == "connectivity")
    assert connectivity.severity == "critical"


def test_get_health_events_capacity_reflects_bounded_queue_over_threshold():
    from activemq_adapter.models import Resource

    adapter = _adapter()
    broker_attrs = {"Name": "0.0.0.0", "Started": True, "Active": True, "AddressMemoryUsage": 0, "GlobalMaxSize": 0}
    over = Resource(id="x", broker_id="b1", namespace="addr", name="hot-queue", depth_current=96, depth_max=100)
    with patch.object(adapter, "_broker_mbean_name", return_value="broker-mbean"):
        with patch.object(adapter, "_read", return_value=broker_attrs):
            events = adapter.get_health_events(resources=[over])

    depth_event = next(e for e in events if e.category == "capacity" and "hot-queue" in e.message)
    assert depth_event.severity == "critical"


def test_get_health_events_uses_provided_resources_without_refetching():
    adapter = _adapter()
    broker_attrs = {"Name": "0.0.0.0", "Started": True, "Active": True, "AddressMemoryUsage": 0, "GlobalMaxSize": 0}
    with patch.object(adapter, "_broker_mbean_name", return_value="broker-mbean"):
        with patch.object(adapter, "_read", return_value=broker_attrs):
            with patch.object(adapter, "get_resources") as mock_resources:
                adapter.get_health_events(resources=[])
    mock_resources.assert_not_called()
