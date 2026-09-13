"""CLI for exercising the RabbitMQ adapter against a real broker.

Stands in for "wire it into the ingestion/poller service" (see
/docs/architecture.md §4) — prints the normalized model as JSON.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys

import requests

from .client import RabbitMQAdapter, resource_id
from .peek import peek_messages


def _to_json(obj):
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    raise TypeError(f"not JSON serializable: {obj!r}")


def cmd_queues(args: argparse.Namespace) -> None:
    adapter = RabbitMQAdapter(
        broker_id=args.broker_id,
        base_url=args.api_url,
        vhost=args.vhost,
        username=args.username,
        password=args.password,
    )
    try:
        out = {
            "resources": adapter.get_resources(),
            "health_events": adapter.get_health_events(),
        }
        print(json.dumps(out, default=_to_json, indent=2))
    finally:
        adapter.close()


def cmd_peek(args: argparse.Namespace) -> None:
    if not args.vhost:
        raise SystemExit("peek needs --vhost: pick which vhost the queue lives in (unlike `queues`, which can discover all)")
    session = requests.Session()
    session.auth = (args.username, args.password)
    try:
        samples = peek_messages(
            session=session,
            base_url=args.api_url,
            vhost=args.vhost,
            queue_name=args.queue,
            resource_id=resource_id(args.broker_id, args.vhost, args.queue),
            limit=args.limit,
        )
    finally:
        session.close()
    print(json.dumps(samples, default=_to_json, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rabbitmq-adapter")
    parser.add_argument("--broker-id", default="local-rabbitmq", help="id to tag normalized output with (Broker.id)")
    parser.add_argument("--vhost", default=None, help="virtual host name — restricts `queues` to one vhost (omit to discover all); required for `peek`")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="admin")
    parser.add_argument("--api-url", default="http://localhost:15672", help="management API base URL")
    sub = parser.add_subparsers(dest="command", required=True)

    p_queues = sub.add_parser("queues", help="list resources and health as normalized JSON (management API)")
    p_queues.set_defaults(func=cmd_queues)

    p_peek = sub.add_parser("peek", help="non-destructive get of a queue's messages (ackmode=ack_requeue_true)")
    p_peek.add_argument("queue")
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
