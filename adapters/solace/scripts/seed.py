#!/usr/bin/env python3
"""Seed a Solace broker with a test queue and a few messages, so
`solace-adapter queues` and `solace-adapter peek` have something real to
show. Not part of the adapter itself; dev-only tooling.

Uses SEMP config (queue create) + REST messaging (publish) — no
solace-pubsubplus dependency needed just to seed data.
"""

from __future__ import annotations

import sys

import requests

SEMP_URL = "http://localhost:8080"
REST_MESSAGING_URL = "http://localhost:9000"
VPN = "default"
QUEUE = "one-msg-ui-test"
AUTH = ("admin", "admin")


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    resp = session.post(
        f"{SEMP_URL}/SEMP/v2/config/msgVpns/{VPN}/queues",
        json={
            "queueName": QUEUE,
            "accessType": "exclusive",
            "maxMsgSpoolUsage": 1000,
            "permission": "consume",
            "ingressEnabled": True,
            "egressEnabled": True,
        },
    )
    if resp.status_code == 200:
        print(f"created queue {QUEUE!r}")
    elif resp.status_code == 400 and "already exists" in resp.text.lower():
        print(f"queue {QUEUE!r} already exists, reusing it")
    else:
        resp.raise_for_status()

    for i in range(1, 5):
        pub = session.post(
            f"{REST_MESSAGING_URL}/QUEUE/{QUEUE}",
            headers={"Content-Type": "application/json", "Solace-Delivery-Mode": "persistent"},
            data=f'{{"order_id": {i}, "amount": {i}.5}}',
        )
        pub.raise_for_status()
    print("published 4 messages")

    q = session.get(f"{SEMP_URL}/SEMP/v2/monitor/msgVpns/{VPN}/queues/{QUEUE}").json()["data"]
    print(f"queue now has {q['spooledMsgCount']} spooled message(s), {q['spooledByteCount']} bytes")


if __name__ == "__main__":
    sys.exit(main() or 0)
