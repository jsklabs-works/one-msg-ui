from unittest.mock import MagicMock

from rabbitmq_adapter.peek import peek_messages


def _session(json_body):
    session = MagicMock()
    session.post.return_value.json.return_value = json_body
    session.post.return_value.raise_for_status.return_value = None
    return session


def test_peek_messages_maps_management_api_get_response():
    body = [
        {
            "payload": '{"order_id": 1}',
            "payload_encoding": "string",
            "payload_bytes": 15,
            "routing_key": "orders",
            "properties": {"message_id": "abc-1", "timestamp": 12345, "headers": {"trace-id": "t1"}},
        }
    ]
    session = _session(body)
    samples = peek_messages(session, "http://example", "/", "orders", resource_id="r1", limit=5)

    assert len(samples) == 1
    s = samples[0]
    assert s.resource_id == "r1"
    assert s.message_id == "abc-1"
    assert s.timestamp == 12345
    assert s.topic == "orders"
    assert s.headers == {"trace-id": "t1"}
    assert s.body_preview == '{"order_id": 1}'
    assert s.size_bytes == 15


def test_peek_messages_calls_get_with_ack_requeue_true_and_the_right_path():
    session = _session([])
    peek_messages(session, "http://example", "/", "orders", resource_id="r1", limit=7)

    session.post.assert_called_once()
    call = session.post.call_args
    assert call.args[0] == "http://example/api/queues/%2F/orders/get"
    assert call.kwargs["json"]["count"] == 7
    assert call.kwargs["json"]["ackmode"] == "ack_requeue_true"


def test_peek_messages_decodes_base64_payload():
    body = [{"payload": "aGVsbG8=", "payload_encoding": "base64", "payload_bytes": 5, "routing_key": None, "properties": {}}]
    session = _session(body)
    samples = peek_messages(session, "http://example", "/", "q", resource_id="r1")
    assert samples[0].body_preview == "hello"


def test_peek_messages_no_message_id_falls_back_to_none():
    body = [{"payload": "x", "payload_encoding": "string", "payload_bytes": 1, "routing_key": "rk", "properties": {}}]
    session = _session(body)
    samples = peek_messages(session, "http://example", "/", "q", resource_id="r1")
    assert samples[0].message_id is None


def test_peek_messages_returns_empty_list_for_empty_queue():
    session = _session([])
    samples = peek_messages(session, "http://example", "/", "q", resource_id="r1")
    assert samples == []
