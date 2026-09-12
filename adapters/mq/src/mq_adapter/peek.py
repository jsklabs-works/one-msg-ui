"""Non-destructive message browsing — architecture.md §3.1.

Verified empirically against a real queue manager: the REST Messaging
API's plain `GET .../queue/{q}/message` does NOT remove the message it
returns — repeated GETs against a queue with multiple messages kept
returning the same (oldest) message and queue depth never changed. No
special "browse" query parameter exists or is needed; a destructive
consume would need a different mechanism (a stateful "queue consumer"
sub-resource, or MQI's MQGET) that this adapter deliberately never uses.

Real limitation, not worked around: this endpoint has no browse cursor.
It always returns the same head-of-queue message, so unlike the Kafka
and Solace adapters' `peek_messages(limit=N)`, this can only ever surface
one message per queue — the oldest — not the next N. Getting a real
multi-message non-destructive browse (MQGMO_BROWSE_NEXT) needs pymqi/MQI,
which was deliberately not used here to avoid its native-client
dependency risk (see adapters/mq/README.md).
"""

from __future__ import annotations

from .models import MessageSample

_PREVIEW_MAX_BYTES = 2048
_CSRF_HEADER = {"ibm-mq-rest-csrf-token": "one-msg-ui"}


def _preview(body: bytes) -> str:
    truncated = body[:_PREVIEW_MAX_BYTES]
    try:
        text = truncated.decode("utf-8")
    except UnicodeDecodeError:
        text = truncated.hex()
    if len(body) > _PREVIEW_MAX_BYTES:
        text += "... [truncated]"
    return text


def peek_messages(session, base_url: str, qmgr_name: str, queue_name: str, resource_id: str, limit: int = 1) -> list[MessageSample]:
    """Return the queue's oldest message, without removing it — see the
    module docstring for why `limit` beyond 1 can't be honored here.
    `session` is a `requests.Session` (reuse MQAdapter's, or pass a fresh
    one configured with the same auth/verify settings).
    """
    resp = session.get(
        f"{base_url}/ibmmq/rest/v1/messaging/qmgr/{qmgr_name}/queue/{queue_name}/message",
        headers=_CSRF_HEADER,
        timeout=10,
    )
    if resp.status_code == 204:
        return []
    resp.raise_for_status()

    body = resp.content
    return [
        MessageSample(
            resource_id=resource_id,
            message_id=resp.headers.get("ibm-mq-md-messageid"),
            timestamp=None,  # not exposed by this endpoint; see README
            headers={
                k: v
                for k, v in resp.headers.items()
                if k.lower().startswith("ibm-mq-md-")
            },
            body_preview=_preview(body),
            size_bytes=len(body),
        )
    ]
