from unittest.mock import patch

from rabbitmq_adapter.client import RabbitMQAdapter, resource_id


def test_resource_id_escapes_the_literal_default_vhost():
    # A raw "/" breaks the API layer's /api/resources/{resource_id} route
    # (see resource_id's docstring) — confirmed live, not just theorized.
    assert resource_id("b1", "/", "orders") == "b1:__default__:orders"


def test_resource_id_leaves_other_vhost_names_unchanged():
    assert resource_id("b1", "tenant-a", "orders") == "b1:tenant-a:orders"


def _adapter(**kwargs):
    return RabbitMQAdapter(broker_id="b1", base_url="http://example", username="u", password="p", **kwargs)


def test_list_vhost_names_returns_configured_vhost_without_an_http_call():
    adapter = _adapter(vhost="/")
    with patch.object(adapter, "_get") as mock_get:
        names = adapter._list_vhost_names()
    assert names == ["/"]
    mock_get.assert_not_called()


def test_list_vhost_names_discovers_all_vhosts_when_unset():
    adapter = _adapter()
    with patch.object(adapter, "_get", return_value=[{"name": "/"}, {"name": "tenant-a"}]) as mock_get:
        names = adapter._list_vhost_names()
    assert names == ["/", "tenant-a"]
    mock_get.assert_called_once_with("/api/vhosts")


def test_get_resources_maps_management_api_fields_to_resource_shape():
    adapter = _adapter(vhost="/")
    queue_data = [
        {"name": "orders", "messages": 5, "consumers": 2, "arguments": {"x-max-length": 1000}},
    ]
    with patch.object(adapter, "_get", return_value=queue_data) as mock_get:
        resources = adapter.get_resources()

    mock_get.assert_called_once_with("/api/queues/%2F")
    assert len(resources) == 1
    r = resources[0]
    # Escaped in the id (see resource_id()'s docstring — a raw "/" breaks
    # /api/resources/{resource_id} routing) but namespace stays the real "/".
    assert r.id == "b1:__default__:orders"
    assert r.namespace == "/"
    assert r.name == "orders"
    assert r.kind == "queue"
    assert r.depth_current == 5
    assert r.depth_max == 1000
    assert r.consumer_count == 2
    assert r.consumer_lag is None


def test_get_resources_leaves_depth_max_none_when_no_max_length_policy():
    # The common case — most RabbitMQ queues have no length limit at all.
    adapter = _adapter(vhost="/")
    with patch.object(adapter, "_get", return_value=[{"name": "unbounded", "messages": 3, "arguments": {}}]):
        resources = adapter.get_resources()
    assert resources[0].depth_max is None


def test_get_resources_spans_every_discovered_vhost():
    adapter = _adapter()
    by_path = {
        "/api/vhosts": [{"name": "/"}, {"name": "tenant-a"}],
        "/api/queues/%2F": [{"name": "q1", "messages": 1}],
        "/api/queues/tenant-a": [{"name": "q2", "messages": 2}],
    }
    with patch.object(adapter, "_get", side_effect=lambda path: by_path[path]):
        resources = adapter.get_resources()
    assert {(r.namespace, r.name) for r in resources} == {("/", "q1"), ("tenant-a", "q2")}


def test_get_health_events_covers_every_node_and_appends_one_capacity_event():
    adapter = _adapter(vhost="/")
    nodes = [
        {"name": "rabbit@node1", "running": True, "mem_alarm": False, "disk_free_alarm": False},
        {"name": "rabbit@node2", "running": True, "mem_alarm": False, "disk_free_alarm": False},
    ]
    with patch.object(adapter, "_get", return_value=nodes):
        events = adapter.get_health_events(resources=[])

    connectivity = [e for e in events if e.category == "connectivity"]
    capacity = [e for e in events if e.category == "capacity"]
    assert len(connectivity) == 2
    assert all(e.severity == "ok" for e in connectivity)
    assert len(capacity) == 1
    assert "no queue length limits" in capacity[0].message


def test_get_health_events_flags_a_down_node_as_critical():
    adapter = _adapter(vhost="/")
    nodes = [{"name": "rabbit@node1", "running": False, "mem_alarm": False, "disk_free_alarm": False}]
    with patch.object(adapter, "_get", return_value=nodes):
        events = adapter.get_health_events(resources=[])
    connectivity = next(e for e in events if e.category == "connectivity")
    assert connectivity.severity == "critical"


def test_get_health_events_capacity_reflects_bounded_queue_over_threshold():
    from rabbitmq_adapter.models import Resource

    adapter = _adapter(vhost="/")
    over = Resource(id="x", broker_id="b1", namespace="/", name="hot-queue", depth_current=96, depth_max=100)
    with patch.object(adapter, "_get", return_value=[]):
        events = adapter.get_health_events(resources=[over])
    capacity = next(e for e in events if e.category == "capacity")
    assert capacity.severity == "critical"
    assert "hot-queue" in capacity.message


def test_get_health_events_uses_provided_resources_without_refetching():
    adapter = _adapter(vhost="/")
    with patch.object(adapter, "_get", return_value=[{"name": "n", "running": True, "mem_alarm": False, "disk_free_alarm": False}]) as mock_get:
        with patch.object(adapter, "get_resources") as mock_resources:
            adapter.get_health_events(resources=[])
    mock_resources.assert_not_called()
    mock_get.assert_called_once_with("/api/nodes")
