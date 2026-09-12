"""Non-destructive message browsing — architecture.md §3.1.

Uses a scratch consumer with no group membership (assign() + manual
seek()) and enable_auto_commit=False. This adapter must never commit an
offset on this path — that's the whole safety property that makes this a
"peek" instead of a consume. Do not add group-based `subscribe()` here.
"""

from __future__ import annotations

import uuid

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

from .models import MessageSample

_PREVIEW_MAX_BYTES = 2048


def _preview(value: bytes | None) -> str:
    if value is None:
        return ""
    truncated = value[:_PREVIEW_MAX_BYTES]
    try:
        text = truncated.decode("utf-8")
    except UnicodeDecodeError:
        text = truncated.hex()
    if len(value) > _PREVIEW_MAX_BYTES:
        text += "... [truncated]"
    return text


def peek_messages(
    bootstrap_servers: str,
    resource_id: str,
    topic: str,
    limit: int = 10,
    partition: int | None = None,
    from_end: bool = True,
) -> list[MessageSample]:
    """Return up to `limit` recent messages from `topic` without consuming
    them (no committed offset, no group state left behind).

    from_end=True (default) reads the most recent `limit` messages per
    partition — the common "what's on this queue right now" support view.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=None,  # no group membership at all — nothing to commit against
        enable_auto_commit=False,
        client_id=f"one-msg-ui-peek-{uuid.uuid4().hex[:8]}",
        consumer_timeout_ms=3000,
    )
    try:
        partitions = [partition] if partition is not None else sorted(consumer.partitions_for_topic(topic) or [])
        tps = [TopicPartition(topic, p) for p in partitions]
        if not tps:
            return []
        consumer.assign(tps)

        end_offsets = consumer.end_offsets(tps)
        beginning_offsets = consumer.beginning_offsets(tps)

        for tp in tps:
            end = end_offsets.get(tp, 0)
            begin = beginning_offsets.get(tp, 0)
            if from_end:
                start = max(end - limit, begin)
            else:
                start = begin
            consumer.seek(tp, start)

        samples: list[MessageSample] = []
        for record in consumer:
            headers = {k: (v.decode("utf-8", "replace") if isinstance(v, bytes) else str(v)) for k, v in (record.headers or [])}
            samples.append(
                MessageSample(
                    resource_id=resource_id,
                    partition=record.partition,
                    offset=record.offset,
                    timestamp=record.timestamp,
                    headers=headers,
                    body_preview=_preview(record.value),
                    size_bytes=len(record.value) if record.value is not None else 0,
                )
            )
            if len(samples) >= limit:
                break
        return samples
    finally:
        # Never commit — close() with no prior commit_sync/commit_async call.
        consumer.close(autocommit=False)
