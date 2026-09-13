from unittest.mock import patch

from solace_adapter.client import SolaceAdapter


def _adapter():
    return SolaceAdapter(broker_id="b1", base_url="http://example", vpn_name="default", username="u", password="p")


def test_get_all_pages_follows_cursor_until_exhausted():
    pages = [
        {"data": [{"queueName": "q1"}], "meta": {"paging": {"cursorQuery": "cursor-2"}}},
        {"data": [{"queueName": "q2"}], "meta": {"paging": {"cursorQuery": "cursor-3"}}},
        {"data": [{"queueName": "q3"}], "meta": {}},  # no cursor -> stop
    ]
    adapter = _adapter()
    with patch.object(adapter, "_get", side_effect=pages) as mock_get:
        items = adapter._get_all_pages("/some/path")

    assert [i["queueName"] for i in items] == ["q1", "q2", "q3"]
    assert mock_get.call_count == 3
    # second call should carry the cursor returned by the first page
    assert mock_get.call_args_list[1].kwargs["params"]["cursor"] == "cursor-2"


def test_get_all_pages_single_page_when_no_cursor():
    adapter = _adapter()
    with patch.object(adapter, "_get", return_value={"data": [{"queueName": "only"}], "meta": {}}) as mock_get:
        items = adapter._get_all_pages("/some/path")

    assert [i["queueName"] for i in items] == ["only"]
    assert mock_get.call_count == 1


def test_get_resources_maps_semp_fields_to_resource_shape():
    adapter = _adapter()
    queue_data = [
        {"queueName": "orders", "spooledMsgCount": 5, "maxMsgSpoolUsage": 1000, "spooledByteCount": 512},
    ]
    with patch.object(adapter, "_get_all_pages", return_value=queue_data) as mock_pages:
        resources = adapter.get_resources()

    mock_pages.assert_called_once_with("/SEMP/v2/monitor/msgVpns/default/queues")
    assert len(resources) == 1
    r = resources[0]
    assert r.id == "b1:default:orders"
    assert r.namespace == "default"
    assert r.name == "orders"
    assert r.kind == "queue"
    assert r.depth_current == 5
    assert r.depth_max == 1000
    assert r.spooled_bytes == 512
    assert r.consumer_lag is None  # no per-consumer offset model for Solace queues


# -- Multi-VPN discovery -----------------------------------------------------
# The bug this covers: a broker connection pinned to one hardcoded VPN never
# sees a VPN created on the broker afterward. vpn_name is now an optional
# filter — unset means "discover every VPN these credentials can see".


def test_list_vpn_names_returns_configured_vpn_without_an_http_call():
    adapter = SolaceAdapter(broker_id="b1", base_url="http://example", username="u", password="p", vpn_name="default")
    with patch.object(adapter, "_get_all_pages") as mock_pages:
        names = adapter._list_vpn_names()
    assert names == ["default"]
    mock_pages.assert_not_called()


def test_list_vpn_names_discovers_all_vpns_when_unset():
    adapter = SolaceAdapter(broker_id="b1", base_url="http://example", username="u", password="p")
    with patch.object(adapter, "_get_all_pages", return_value=[{"msgVpnName": "default"}, {"msgVpnName": "testVPN"}]) as mock_pages:
        names = adapter._list_vpn_names()
    assert names == ["default", "testVPN"]
    mock_pages.assert_called_once_with("/SEMP/v2/monitor/msgVpns")


def test_get_resources_spans_every_discovered_vpn():
    adapter = SolaceAdapter(broker_id="b1", base_url="http://example", username="u", password="p")
    queues_by_path = {
        "/SEMP/v2/monitor/msgVpns": [{"msgVpnName": "default"}, {"msgVpnName": "testVPN"}],
        "/SEMP/v2/monitor/msgVpns/default/queues": [{"queueName": "q1"}],
        "/SEMP/v2/monitor/msgVpns/testVPN/queues": [{"queueName": "q2"}],
    }
    with patch.object(adapter, "_get_all_pages", side_effect=lambda path, params=None: queues_by_path[path]):
        resources = adapter.get_resources()

    assert {(r.namespace, r.name) for r in resources} == {("default", "q1"), ("testVPN", "q2")}


def test_get_health_events_covers_every_discovered_vpn():
    adapter = SolaceAdapter(broker_id="b1", base_url="http://example", username="u", password="p")
    vpn_bodies = {
        "/SEMP/v2/monitor/msgVpns/default": {"data": {"state": "up", "enabled": True}},
        "/SEMP/v2/monitor/msgVpns/testVPN": {"data": {"state": "up", "enabled": True}},
    }
    with patch.object(adapter, "_get_all_pages", return_value=[{"msgVpnName": "default"}, {"msgVpnName": "testVPN"}]):
        with patch.object(adapter, "_get", side_effect=lambda path, params=None: vpn_bodies[path]):
            events = adapter.get_health_events()

    connectivity_events = [e for e in events if e.category == "connectivity"]
    assert len(connectivity_events) == 2
    assert {"default" in e.message or "testVPN" in e.message for e in connectivity_events} == {True}
