from solace_adapter.peek import _PREVIEW_MAX_BYTES, _preview


def test_preview_passes_through_short_string():
    assert _preview("hello") == "hello"


def test_preview_handles_none_payload():
    assert _preview(None) == ""


def test_preview_decodes_utf8_bytes():
    assert _preview(b'{"order_id": 1}') == '{"order_id": 1}'


def test_preview_falls_back_to_hex_for_non_utf8_bytes():
    raw = b"\xff\xfe\x00\x01"
    assert _preview(raw) == raw.hex()


def test_preview_truncates_long_string_bodies():
    body = "x" * (_PREVIEW_MAX_BYTES + 100)
    result = _preview(body)
    assert result.endswith("... [truncated]")
    assert len(result) < len(body)
