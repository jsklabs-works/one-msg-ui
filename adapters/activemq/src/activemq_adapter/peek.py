"""Non-destructive message browsing — architecture.md §3.1.

The queue MBean's `browse()` operation, invoked over Jolokia, is a real
broker-side non-destructive read — Artemis's own JMS-style queue browser
under the hood, not a consume-and-hope workaround. Confirmed live: queue
`MessageCount` was unchanged after browsing all 4 test messages, and
browsing again returned the same 4 messages — repeatable, like Kafka's/
Solace's/RabbitMQ's peek, unlike MQ's one-shot head-of-queue limitation.

Real limitation: `browse()`'s JSON only carries a `text` field for
TEXT-type messages. A BytesMessage/ObjectMessage/StreamMessage has no
body field in this API surface at all — this adapter reports that
honestly (`"[non-text message, N bytes]"`) rather than guessing at a
decode.
"""

from __future__ import annotations

from .models import MessageSample

_PREVIEW_MAX_BYTES = 2048
# Verified empirically: a trailing bare "*" is what actually lets
# unlisted properties (broker=, routing-type=) through a JMX ObjectName
# search pattern — a wildcarded *value* on a named key
# (routing-type=*) does NOT do the same thing and matches nothing,
# since Jolokia needs the full property *set* to line up unless a
# trailing "*" says "and anything else."
_QUEUE_MBEAN_PATTERN = 'org.apache.activemq.artemis:address="{address}",component=addresses,queue="{queue}",subcomponent=queues,*'


def _preview(text: str) -> str:
    if len(text.encode("utf-8", errors="ignore")) > _PREVIEW_MAX_BYTES:
        return text[:_PREVIEW_MAX_BYTES] + "... [truncated]"
    return text


def peek_messages(session, base_url: str, address: str, queue_name: str, resource_id: str, limit: int = 10):
    """Return up to `limit` messages from `queue_name` (bound to
    `address`), without removing them. `session` is a `requests.Session`
    (reuse ActiveMQAdapter's, or pass a fresh one with the same auth/
    verify settings).

    Real limitation: `browse()` has no server-side limit/pagination — it
    always returns the whole queue, and `limit` is applied client-side
    after the fact. Fine for inspection-sized queues; a queue with
    thousands of messages means a proportionally large response before
    this ever gets to slice it down.
    """
    jolokia_url = f"{base_url.rstrip('/')}/console/jolokia/"

    # routing-type is part of the queue's canonical ObjectName but not
    # something a peek caller should need to know up front (Resource
    # doesn't carry it) — search with a wildcard instead of guessing
    # anycast vs. multicast.
    search_resp = session.post(
        jolokia_url,
        json={"type": "search", "mbean": _QUEUE_MBEAN_PATTERN.format(address=address, queue=queue_name)},
        timeout=10,
    )
    search_resp.raise_for_status()
    search_body = search_resp.json()
    if search_body.get("status") != 200:
        raise RuntimeError(f"couldn't find queue MBean for {address}/{queue_name}: {search_body}")
    mbeans = search_body["value"]
    if not mbeans:
        raise RuntimeError(f"no such queue: address={address!r} queue={queue_name!r}")

    browse_resp = session.post(jolokia_url, json={"type": "exec", "mbean": mbeans[0], "operation": "browse()"}, timeout=10)
    browse_resp.raise_for_status()
    browse_body = browse_resp.json()
    if browse_body.get("status") != 200:
        raise RuntimeError(f"browse failed for {address}/{queue_name}: {browse_body}")

    samples = []
    for m in browse_body["value"][:limit]:
        text = m.get("text")
        size_bytes = m.get("persistentSize") or 0
        body_preview = _preview(text) if text is not None else f"[non-text message, {size_bytes} bytes]"

        headers: dict[str, str] = {}
        for bucket in ("StringProperties", "IntProperties", "LongProperties", "BooleanProperties", "DoubleProperties", "FloatProperties", "ShortProperties", "ByteProperties"):
            for k, v in (m.get(bucket) or {}).items():
                headers[str(k)] = str(v)

        samples.append(
            MessageSample(
                resource_id=resource_id,
                message_id=str(m["messageID"]) if m.get("messageID") is not None else None,
                timestamp=m.get("timestamp"),
                topic=m.get("address"),
                headers=headers,
                body_preview=body_preview,
                size_bytes=size_bytes,
            )
        )
    return samples
