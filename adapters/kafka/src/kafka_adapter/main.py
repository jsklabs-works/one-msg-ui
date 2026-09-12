"""CLI for exercising the Kafka adapter against a real cluster.

This stands in for "wire it into the ingestion/poller service" (see
/docs/architecture.md §4) until that service exists — it prints the
normalized model as JSON so it's easy to eyeball or pipe elsewhere.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys

from .client import KafkaAdapter
from .peek import peek_messages


def _to_json(obj):
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    raise TypeError(f"not JSON serializable: {obj!r}")


def cmd_topics(args: argparse.Namespace) -> None:
    adapter = KafkaAdapter(broker_id=args.broker_id, bootstrap_servers=args.bootstrap_servers)
    try:
        groups = adapter.get_consumer_groups()
        resources = adapter.get_resources(consumer_groups=groups)
        health = adapter.get_health_events()
        out = {
            "resources": resources,
            "consumer_groups": groups,
            "health_events": health,
        }
        print(json.dumps(out, default=_to_json, indent=2))
    finally:
        adapter.close()


def cmd_peek(args: argparse.Namespace) -> None:
    samples = peek_messages(
        bootstrap_servers=args.bootstrap_servers,
        resource_id=f"{args.broker_id}:{args.topic}",
        topic=args.topic,
        limit=args.limit,
        partition=args.partition,
    )
    print(json.dumps(samples, default=_to_json, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kafka-adapter")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--broker-id", default="local-kafka", help="id to tag normalized output with (Broker.id)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_topics = sub.add_parser("topics", help="list resources, consumer groups, and health as normalized JSON")
    p_topics.set_defaults(func=cmd_topics)

    p_peek = sub.add_parser("peek", help="non-destructive peek at a topic's messages")
    p_peek.add_argument("topic")
    p_peek.add_argument("--limit", type=int, default=10)
    p_peek.add_argument("--partition", type=int, default=None)
    p_peek.set_defaults(func=cmd_peek)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
