"""CLI for exercising the ActiveMQ Artemis adapter against a real broker.

Stands in for "wire it into the ingestion/poller service" (see
/docs/architecture.md §4) — prints the normalized model as JSON.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys

import requests

from .client import ActiveMQAdapter
from .peek import peek_messages


def _to_json(obj):
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    raise TypeError(f"not JSON serializable: {obj!r}")


def cmd_queues(args: argparse.Namespace) -> None:
    adapter = ActiveMQAdapter(broker_id=args.broker_id, base_url=args.console_url, username=args.username, password=args.password)
    try:
        out = {
            "resources": adapter.get_resources(),
            "health_events": adapter.get_health_events(),
        }
        print(json.dumps(out, default=_to_json, indent=2))
    finally:
        adapter.close()


def cmd_peek(args: argparse.Namespace) -> None:
    session = requests.Session()
    session.auth = (args.username, args.password)
    try:
        samples = peek_messages(
            session=session,
            base_url=args.console_url,
            address=args.address or args.queue,
            queue_name=args.queue,
            resource_id=f"{args.broker_id}:{args.address or args.queue}:{args.queue}",
            limit=args.limit,
        )
    finally:
        session.close()
    print(json.dumps(samples, default=_to_json, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="activemq-adapter")
    parser.add_argument("--broker-id", default="local-activemq", help="id to tag normalized output with (Broker.id)")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="admin")
    parser.add_argument("--console-url", default="http://localhost:8161", help="Artemis web console base URL (Jolokia lives under it)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_queues = sub.add_parser("queues", help="list resources and health as normalized JSON (Jolokia)")
    p_queues.set_defaults(func=cmd_queues)

    p_peek = sub.add_parser("peek", help="non-destructive browse of a queue's messages (QueueControl.browse())")
    p_peek.add_argument("queue")
    p_peek.add_argument("--address", default=None, help="the address this queue is bound to (defaults to the queue name, the common case when auto-created)")
    p_peek.add_argument("--limit", type=int, default=10)
    p_peek.set_defaults(func=cmd_peek)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
