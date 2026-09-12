from mq_adapter.mqsc import parse_command_response, parse_mqsc_line


def test_parse_mqsc_line_extracts_all_attribute_pairs():
    line = "AMQ8409I: Display Queue details.   QUEUE(DEV.QUEUE.1)   TYPE(QLOCAL)   CURDEPTH(0)   MAXDEPTH(5000)"
    assert parse_mqsc_line(line) == {
        "QUEUE": "DEV.QUEUE.1",
        "TYPE": "QLOCAL",
        "CURDEPTH": "0",
        "MAXDEPTH": "5000",
    }


def test_parse_mqsc_line_ignores_the_informational_prefix():
    # "AMQ8409I" has no parenthesized value, so it's naturally excluded.
    line = "AMQ8409I: Display Queue details.   QUEUE(Q1)"
    result = parse_mqsc_line(line)
    assert "AMQ8409I" not in result
    assert result == {"QUEUE": "Q1"}


def test_parse_mqsc_line_empty_when_no_attributes_present():
    assert parse_mqsc_line("AMQ8450I: some message with no parens") == {}


def test_parse_command_response_one_dict_per_matched_object():
    command_response = [
        {"text": ["AMQ8409I: Display Queue details.   QUEUE(Q1)   CURDEPTH(0)"]},
        {"text": ["AMQ8409I: Display Queue details.   QUEUE(Q2)   CURDEPTH(5)"]},
    ]
    result = parse_command_response(command_response)
    assert result == [{"QUEUE": "Q1", "CURDEPTH": "0"}, {"QUEUE": "Q2", "CURDEPTH": "5"}]


def test_parse_command_response_joins_multiple_text_lines_per_item():
    # MQSC output can wrap across multiple "text" entries for one object.
    command_response = [{"text": ["AMQ8409I: Display Queue details.   QUEUE(Q1)", "CURDEPTH(3)   MAXDEPTH(100)"]}]
    result = parse_command_response(command_response)
    assert result == [{"QUEUE": "Q1", "CURDEPTH": "3", "MAXDEPTH": "100"}]


def test_parse_command_response_skips_items_with_no_attributes():
    # e.g. a pure error/informational text block with nothing to extract.
    command_response = [
        {"text": ["AMQ8409I: Display Queue details.   QUEUE(Q1)   CURDEPTH(1)"]},
        {"text": ["some informational line with no attributes"]},
    ]
    result = parse_command_response(command_response)
    assert result == [{"QUEUE": "Q1", "CURDEPTH": "1"}]
