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
    with patch.object(adapter, "_get_all_pages", return_value=queue_data):
        resources = adapter.get_resources()

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
