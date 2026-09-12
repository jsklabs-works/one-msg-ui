"""CLI for exercising the Solace adapter against a real broker.

Stands in for "wire it into the ingestion/poller service" (see
/docs/architecture.md §4) — prints the normalized model as JSON.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys

from .client import SolaceAdapter
from .peek import peek_messages


def _to_json(obj):
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    raise TypeError(f"not JSON serializable: {obj!r}")


def cmd_queues(args: argparse.Namespace) -> None:
    adapter = SolaceAdapter(
        broker_id=args.broker_id,
        base_url=args.semp_url,
        vpn_name=args.vpn,
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
    samples = peek_messages(
        smf_host=args.smf_host,
        vpn_name=args.vpn,
        username=args.username,
        password=args.password,
        resource_id=f"{args.broker_id}:{args.vpn}:{args.queue}",
        queue_name=args.queue,
        limit=args.limit,
    )
    print(json.dumps(samples, default=_to_json, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="solace-adapter")
    parser.add_argument("--broker-id", default="local-solace", help="id to tag normalized output with (Broker.id)")
    parser.add_argument("--vpn", default="default", help="Message VPN name")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="admin")
    sub = parser.add_subparsers(dest="command", required=True)

    p_queues = sub.add_parser("queues", help="list resources and health as normalized JSON (SEMP v2 monitor)")
    p_queues.add_argument("--semp-url", default="http://localhost:8080", help="SEMP v2 base URL")
    p_queues.set_defaults(func=cmd_queues)

    p_peek = sub.add_parser("peek", help="non-destructive browse of a queue's messages (real SMF connection)")
    p_peek.add_argument("queue")
    p_peek.add_argument("--smf-host", default="tcp://localhost:55554", help="SMF host, e.g. tcp://localhost:55554")
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
