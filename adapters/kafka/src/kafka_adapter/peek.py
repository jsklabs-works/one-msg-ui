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

from .client import security_kwargs
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
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: str | None = None,
    sasl_plain_username: str | None = None,
    sasl_plain_password: str | None = None,
    ssl_cafile: str | None = None,
) -> list[MessageSample]:
    """Return up to `limit` recent messages from `topic` without consuming
    them (no committed offset, no group state left behind).

    from_end=True (default) reads the most recent `limit` messages per
    partition — the common "what's on this queue right now" support view.

    security_protocol/sasl_*/ssl_cafile must match whatever the cluster
    actually needs (see client.py's security_kwargs) — this scratch
    consumer authenticates independently of the main KafkaAdapter.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=None,  # no group membership at all — nothing to commit against
        enable_auto_commit=False,
        client_id=f"one-msg-ui-peek-{uuid.uuid4().hex[:8]}",
        consumer_timeout_ms=3000,
        **security_kwargs(security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password, ssl_cafile),
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
