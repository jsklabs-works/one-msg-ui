"""CLI for exercising the MQ adapter against a real queue manager.

Stands in for "wire it into the ingestion/poller service" (see
/docs/architecture.md §4) — prints the normalized model as JSON.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys

import requests
import urllib3

from .client import MQAdapter
from .peek import peek_messages


def _to_json(obj):
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    raise TypeError(f"not JSON serializable: {obj!r}")


def cmd_queues(args: argparse.Namespace) -> None:
    adapter = MQAdapter(
        broker_id=args.broker_id,
        base_url=args.admin_url,
        qmgr_name=args.qmgr,
        username=args.admin_username,
        password=args.admin_password,
    )
    try:
        resources = adapter.get_resources(queue_pattern=args.pattern)
        out = {
            "resources": resources,
            "health_events": adapter.get_health_events(resources=resources),
        }
        print(json.dumps(out, default=_to_json, indent=2))
    finally:
        adapter.close()


def cmd_peek(args: argparse.Namespace) -> None:
    session = requests.Session()
    session.auth = (args.app_username, args.app_password)
    session.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    try:
        samples = peek_messages(
            session=session,
            base_url=args.admin_url,
            qmgr_name=args.qmgr,
            queue_name=args.queue,
            resource_id=f"{args.broker_id}:{args.qmgr}:{args.queue}",
        )
        print(json.dumps(samples, default=_to_json, indent=2))
    finally:
        session.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mq-adapter")
    parser.add_argument("--broker-id", default="local-mq", help="id to tag normalized output with (Broker.id)")
    parser.add_argument("--qmgr", default="QM1", help="queue manager name")
    parser.add_argument("--admin-url", default="https://localhost:9543", help="REST Admin/Messaging API base URL")
    parser.add_argument("--admin-username", default="admin")
    parser.add_argument("--admin-password", default="adminpassw0rd")
    sub = parser.add_subparsers(dest="command", required=True)

    p_queues = sub.add_parser("queues", help="list resources and health as normalized JSON (MQSC-over-REST)")
    p_queues.add_argument("--pattern", default="*", help="MQSC queue-name pattern, e.g. DEV.*")
    p_queues.set_defaults(func=cmd_queues)

    p_peek = sub.add_parser("peek", help="non-destructive peek at a queue's oldest message (REST Messaging API)")
    p_peek.add_argument("queue")
    p_peek.add_argument("--app-username", default="app")
    p_peek.add_argument("--app-password", default="apppassw0rd")
    p_peek.set_defaults(func=cmd_peek)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
