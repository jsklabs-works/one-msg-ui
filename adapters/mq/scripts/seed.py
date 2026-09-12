#!/usr/bin/env python3
"""Seed the local dev queue manager (docker-compose.yml) with a couple of
test messages on DEV.QUEUE.1, so `mq-adapter queues` and `mq-adapter peek`
have something real to show. Not part of the adapter itself; dev-only
tooling. Uses the REST Messaging API's PUT — no separate client needed.
"""

from __future__ import annotations

import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://localhost:9543"
QMGR = "QM1"
QUEUE = "DEV.QUEUE.1"
APP_AUTH = ("app", "apppassw0rd")
CSRF_HEADER = {"ibm-mq-rest-csrf-token": "one-msg-ui"}


def main() -> None:
    session = requests.Session()
    session.auth = APP_AUTH
    session.verify = False

    for i in range(1, 4):
        resp = session.post(
            f"{BASE_URL}/ibmmq/rest/v1/messaging/qmgr/{QMGR}/queue/{QUEUE}/message",
            headers={**CSRF_HEADER, "Content-Type": "text/plain;charset=utf-8"},
            data=f'{{"order_id": {i}, "amount": {i}.5}}',
        )
        resp.raise_for_status()
    print(f"published 3 messages to {QUEUE!r}")


if __name__ == "__main__":
    sys.exit(main() or 0)
