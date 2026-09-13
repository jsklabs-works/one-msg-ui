#!/usr/bin/env python3
"""Seed a RabbitMQ broker with a test queue and a few messages, so
`rabbitmq-adapter queues` and `rabbitmq-adapter peek` have something real
to show. Not part of the adapter itself; dev-only tooling.

Pure management API — same REST surface the adapter itself uses, no AMQP
client needed just to seed data (mirrors adapters/solace/scripts/seed.py).
"""

from __future__ import annotations

import sys

import requests

API_URL = "http://localhost:15672"
VHOST = "/"
QUEUE = "orders"
AUTH = ("admin", "admin")


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    resp = session.put(
        f"{API_URL}/api/queues/%2F/{QUEUE}",
        json={"durable": True, "arguments": {"x-max-length": 1000}},
    )
    resp.raise_for_status()
    print(f"queue {QUEUE!r} ready (created or already existed)")

    for i in range(1, 5):
        pub = session.post(
            f"{API_URL}/api/exchanges/%2F/amq.default/publish",
            json={
                "properties": {"headers": {"trace-id": f"t{i}"}},
                "routing_key": QUEUE,
                "payload": f'{{"order_id": {i}, "amount": {i}.5}}',
                "payload_encoding": "string",
            },
        )
        pub.raise_for_status()
        if not pub.json().get("routed"):
            raise RuntimeError(f"message {i} did not route onto {QUEUE!r} — does the queue exist?")
    print("published 4 messages")

    # Management API stats are sampled, not instant — a fresh GET right
    # after publishing can still show the pre-publish count (confirmed
    # live: took a couple of seconds to catch up during adapter testing).
    import time

    time.sleep(2)
    q = session.get(f"{API_URL}/api/queues/%2F/{QUEUE}").json()
    print(f"queue now has {q['messages']} message(s)")


if __name__ == "__main__":
    sys.exit(main() or 0)
