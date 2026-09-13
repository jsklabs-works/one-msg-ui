#!/usr/bin/env python3
"""Seed an ActiveMQ Artemis broker with a test queue and a few messages,
so `activemq-adapter queues` and `activemq-adapter peek` have something
real to show. Not part of the adapter itself; dev-only tooling.

Unlike the other three adapters' seed scripts, this one shells out to the
broker's own `artemis` CLI (via `docker exec`) rather than hitting a REST
endpoint directly: Artemis's JMX createQueue has several overloaded
signatures, and getting the ANYCAST (point-to-point, not pub/sub)
variant right from Jolokia alone needs the exact overload's argument
order — the CLI already wraps this correctly, so there's no reason to
reverse-engineer it just for a seed script. The adapter itself remains
pure REST/Jolokia throughout (see client.py/peek.py) — this is dev
tooling's own pragmatic choice, not a dependency the adapter carries.
"""

from __future__ import annotations

import subprocess
import sys

CONTAINER = "one-msg-ui-activemq"
ARTEMIS_BIN = "/var/lib/artemis-instance/bin/artemis"
ADDRESS = "orders"
QUEUE = "orders"
AUTH = ["--user", "admin", "--password", "admin"]


def _artemis(*args: str, input_text: str | None = None) -> None:
    subprocess.run(
        ["docker", "exec", "-i", CONTAINER, ARTEMIS_BIN, *args, *AUTH],
        input=input_text,
        text=True,
        check=True,
    )


def main() -> None:
    try:
        _artemis(
            "queue",
            "create",
            "--name",
            QUEUE,
            "--address",
            ADDRESS,
            "--auto-create-address",
            "--anycast",
            "--durable",
            input_text="N\n",  # answers the interactive --purge-on-no-consumers prompt
        )
        print(f"queue {QUEUE!r} ready")
    except subprocess.CalledProcessError as exc:
        # A second run against an already-seeded broker hits "already
        # exists" — not a real failure, same tolerance the other seed
        # scripts give a pre-existing queue/VPN.
        print(f"queue create exited {exc.returncode} (already exists? continuing)")

    for i in range(1, 5):
        _artemis("producer", "--url", "tcp://localhost:61616", "--destination", f"queue://{QUEUE}", "--message-count", "1", "--message", f'{{"order_id": {i}, "amount": {i}.5}}')
    print("published 4 messages")


if __name__ == "__main__":
    sys.exit(main() or 0)
