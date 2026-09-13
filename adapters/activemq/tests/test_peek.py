from unittest.mock import MagicMock

from activemq_adapter.peek import peek_messages


def _session(search_value, browse_value):
    session = MagicMock()

    def post(url, json, timeout):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        if json["type"] == "search":
            resp.json.return_value = {"status": 200, "value": search_value}
        else:
            resp.json.return_value = {"status": 200, "value": browse_value}
        return resp

    session.post.side_effect = post
    return session


def test_peek_messages_maps_browse_response_including_typed_property_buckets():
    search_value = ['org.apache.activemq.artemis:address="orders",component=addresses,queue="orders",subcomponent=queues']
    browse_value = [
        {
            "messageID": 67,
            "timestamp": 12345,
            "address": "orders",
            "text": '{"order_id": 1}',
            "persistentSize": 406,
            "StringProperties": {"traceId": "t1"},
            "IntProperties": {"retryCount": 0},
            "LongProperties": None,
        }
    ]
    session = _session(search_value, browse_value)
    samples = peek_messages(session, "http://example:8161", "orders", "orders", resource_id="r1", limit=5)

    assert len(samples) == 1
    s = samples[0]
    assert s.resource_id == "r1"
    assert s.message_id == "67"
    assert s.timestamp == 12345
    assert s.topic == "orders"
    assert s.headers == {"traceId": "t1", "retryCount": "0"}
    assert s.body_preview == '{"order_id": 1}'
    assert s.size_bytes == 406


def test_peek_messages_reports_non_text_messages_honestly():
    search_value = ["mbean"]
    browse_value = [{"messageID": 1, "timestamp": 1, "address": "a", "text": None, "persistentSize": 999}]
    session = _session(search_value, browse_value)
    samples = peek_messages(session, "http://example:8161", "a", "q", resource_id="r1")
    assert samples[0].body_preview == "[non-text message, 999 bytes]"


def test_peek_messages_applies_limit_client_side():
    search_value = ["mbean"]
    browse_value = [{"messageID": i, "timestamp": i, "address": "a", "text": "x", "persistentSize": 1} for i in range(10)]
    session = _session(search_value, browse_value)
    samples = peek_messages(session, "http://example:8161", "a", "q", resource_id="r1", limit=3)
    assert len(samples) == 3


def test_peek_messages_raises_when_queue_mbean_not_found():
    search_value = []
    session = _session(search_value, [])
    try:
        peek_messages(session, "http://example:8161", "a", "missing-queue", resource_id="r1")
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass
