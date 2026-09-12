from kafka_adapter.peek import _PREVIEW_MAX_BYTES, _preview


def test_preview_decodes_utf8():
    assert _preview(b'{"order_id": 1}') == '{"order_id": 1}'


def test_preview_handles_none_body():
    assert _preview(None) == ""


def test_preview_falls_back_to_hex_for_non_utf8_bytes():
    raw = b"\xff\xfe\x00\x01"
    result = _preview(raw)
    assert result == raw.hex()


def test_preview_truncates_long_bodies():
    body = b"x" * (_PREVIEW_MAX_BYTES + 100)
    result = _preview(body)
    assert result.endswith("... [truncated]")
    assert len(result) < len(body)
