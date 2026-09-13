"""Non-destructive message browsing — architecture.md §3.1.

The management API's `POST .../queues/{vhost}/{queue}/get` endpoint takes
an `ackmode`. `"ack_requeue_true"` delivers each returned message and
immediately requeues it server-side — a genuine non-destructive browse
(confirmed by RabbitMQ's own docs: this is the mode explicitly documented
for "get messages without removing them"), not a consume-then-hope
workaround. Same REST session as client.py's monitoring calls; no AMQP
client needed.

One real limitation: like Solace's queue browse, requeued messages don't
preserve original ordering against concurrent consumers — fine for a
peek/inspect tool, not a substitute for a real consumer.
"""

from __future__ import annotations

import base64
from urllib.parse import quote

from .models import MessageSample

_PREVIEW_MAX_BYTES = 2048


def _quote(name: str) -> str:
    # Same encoding client.py's monitoring calls use for vhost/queue path
    # segments — kept duplicated rather than cross-imported, matching how
    # the other adapters keep client.py and peek.py independent of each
    # other's internals.
    return quote(name, safe="")


def _preview(payload: str, encoding: str) -> str:
    if encoding == "base64":
        raw = base64.b64decode(payload)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.hex()
    else:
        text = payload
    if len(text.encode("utf-8", errors="ignore")) > _PREVIEW_MAX_BYTES:
        text = text[:_PREVIEW_MAX_BYTES] + "... [truncated]"
    return text


def peek_messages(session, base_url: str, vhost: str, queue_name: str, resource_id: str, limit: int = 10) -> list[MessageSample]:
    """Return up to `limit` messages from `queue_name`, without removing
    them. `session` is a `requests.Session` (reuse RabbitMQAdapter's, or
    pass a fresh one configured with the same auth/verify settings).
    """
    resp = session.post(
        f"{base_url}/api/queues/{_quote(vhost)}/{_quote(queue_name)}/get",
        json={"count": limit, "ackmode": "ack_requeue_true", "encoding": "auto", "truncate": 100_000},
        timeout=10,
    )
    resp.raise_for_status()

    samples = []
    for m in resp.json():
        payload = m.get("payload", "")
        encoding = m.get("payload_encoding", "string")
        properties = m.get("properties") or {}
        message_id = properties.get("message_id")
        samples.append(
            MessageSample(
                resource_id=resource_id,
                message_id=str(message_id) if message_id is not None else None,
                timestamp=properties.get("timestamp"),
                topic=m.get("routing_key"),
                headers={str(k): str(v) for k, v in (properties.get("headers") or {}).items()},
                body_preview=_preview(payload, encoding),
                size_bytes=m.get("payload_bytes", len(payload)),
            )
        )
    return samples
