"""Parses MQSC-over-REST command output into plain dicts.

Kept separate from client.py's HTTP calls so it's unit-testable without a
live queue manager (see tests/test_mqsc.py).

Why this exists: verified empirically that the REST Admin API's plain
resource endpoints (`GET .../queue/{name}`) only expose *configuration*
attributes, not runtime status — `curdepth`/`maxdepth`/`ipprocs` are
rejected as invalid attribute names there. Runtime status instead comes
through the "run an MQSC command over REST" admin action
(`POST .../action/qmgr/{qmgr}/mqsc`), which returns free-text MQSC
output (`KEY(value)` pairs) rather than structured JSON — this is the
REST-first approach the architecture doc anticipated, it just turned out
"REST" here means "REST-wrapped MQSC text", not JSON resource attributes,
for this class of data. No PCF/native client needed either way.
"""

from __future__ import annotations

import re

_ATTR_PATTERN = re.compile(r"([A-Z][A-Z0-9_]*)\(([^()]*)\)")


def parse_mqsc_line(text: str) -> dict[str, str]:
    """Extract KEY(value) pairs from one MQSC response line, e.g.:
    "AMQ8409I: Display Queue details.   QUEUE(DEV.QUEUE.1)  TYPE(QLOCAL)
    CURDEPTH(0)  MAXDEPTH(5000)"
    -> {"QUEUE": "DEV.QUEUE.1", "TYPE": "QLOCAL", "CURDEPTH": "0", "MAXDEPTH": "5000"}

    The leading "AMQ8409I: ..." informational prefix is simply not matched
    by the pattern (no parenthesized value follows it), so it's naturally
    excluded rather than needing to be stripped explicitly.
    """
    return dict(_ATTR_PATTERN.findall(text))


def parse_command_response(command_response: list[dict]) -> list[dict[str, str]]:
    """Parse a full `commandResponse` array (one MQSC-over-REST response)
    into one attribute dict per response item — typically one per matched
    object when the command was a DISPLAY against a wildcard/filter.
    """
    results = []
    for item in command_response:
        joined = " ".join(item.get("text", []))
        parsed = parse_mqsc_line(joined)
        if parsed:
            results.append(parsed)
    return results
