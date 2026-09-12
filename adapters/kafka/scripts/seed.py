#!/usr/bin/env python3
"""Seed the local dev Kafka (docker-compose.yml) with a topic, some
messages, and a consumer group that's deliberately left lagging —
so `kafka-adapter topics` and `kafka-adapter peek` have something real
to show. Not part of the adapter itself; dev-only tooling.
"""

from __future__ import annotations

import json
import sys
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

BOOTSTRAP = "localhost:9092"
TOPIC = "orders"
GROUP = "orders-processor"


def main() -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="seed")
    try:
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)])
        print(f"created topic {TOPIC!r}")
    except Exception as exc:
        print(f"topic create skipped: {exc}")
    finally:
        admin.close()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for i in range(30):
        producer.send(TOPIC, {"order_id": i, "amount": round(i * 3.5, 2)})
    producer.flush()
    producer.close()
    print("produced 30 messages")

    # Consume only the first 10 and commit, so the group is left lagging by
    # ~20 messages — gives the lag calculation something non-zero to report.
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    consumed = 0
    for _ in consumer:
        consumed += 1
        if consumed >= 10:
            break
    consumer.commit()
    consumer.close()
    print(f"consumer group {GROUP!r} committed after {consumed} messages (rest left as lag)")


if __name__ == "__main__":
    sys.exit(main() or 0)
