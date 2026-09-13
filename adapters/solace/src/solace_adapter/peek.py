"""Non-destructive message browsing — architecture.md §3.1.

Confirmed empirically that SEMP v2 monitor cannot do this (its
`/queues/{q}/msgs` endpoints return metadata only — msgId, size,
timestamp — never the payload), so this uses the official
`solace-pubsubplus` client library's MessageQueueBrowser instead: a real
SMF connection using Solace's native, broker-supported non-destructive
browse semantics (messages are read oldest-to-newest and remain on the
queue — `browser.remove()` exists for selective deletion but is
deliberately never called here).
"""

from __future__ import annotations

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.queue import Queue
from solace.messaging.config.solace_properties.authentication_properties import (
    SCHEME_BASIC_PASSWORD,
    SCHEME_BASIC_USER_NAME,
)
from solace.messaging.config.solace_properties.service_properties import VPN_NAME
from solace.messaging.config.solace_properties.transport_layer_properties import HOST
from solace.messaging.config.transport_security_strategy import TLS

from .models import MessageSample

_PREVIEW_MAX_BYTES = 2048
_RECEIVE_TIMEOUT_MS = 3000


def _preview(payload: bytes | str | None) -> str:
    if payload is None:
        return ""
    if isinstance(payload, str):
        raw = payload.encode("utf-8")
        text = payload
    else:
        raw = payload
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.hex()
    if len(raw) > _PREVIEW_MAX_BYTES:
        text = text[:_PREVIEW_MAX_BYTES] + "... [truncated]"
    return text


def peek_messages(
    smf_host: str,
    vpn_name: str,
    username: str,
    password: str,
    resource_id: str,
    queue_name: str,
    limit: int = 10,
    verify_certificate: bool = True,
) -> list[MessageSample]:
    """Return up to `limit` messages from `queue_name`, oldest first,
    without removing them — a proper broker-side browse, not a
    consume-then-not-ack workaround.

    verify_certificate only matters when smf_host uses `tcps://` (TLS) —
    set it False for a broker presenting a self-signed cert (common in
    dev), same intent as the SEMP client's verify_certificate in
    client.py. Harmless to pass either way against a plain `tcp://` host.
    """
    props = {
        HOST: smf_host,
        VPN_NAME: vpn_name,
        SCHEME_BASIC_USER_NAME: username,
        SCHEME_BASIC_PASSWORD: password,
    }
    builder = MessagingService.builder().from_properties(props)
    if not verify_certificate:
        builder = builder.with_transport_security_strategy(TLS.create().without_certificate_validation())
    service = builder.build()
    service.connect()
    try:
        queue = Queue.durable_exclusive_queue(queue_name)
        browser = service.create_message_queue_browser_builder().build(queue)
        browser.start()
        try:
            samples: list[MessageSample] = []
            for _ in range(limit):
                msg = browser.receive_message(timeout=_RECEIVE_TIMEOUT_MS)
                if msg is None:
                    break
                payload = msg.get_payload_as_string()
                if payload is None:
                    payload = msg.get_payload_as_bytes()
                body_preview = _preview(payload)
                size_bytes = len(payload) if payload is not None else 0
                headers = {str(k): str(v) for k, v in (msg.get_properties() or {}).items()}
                # Application-level message id is only populated when the
                # publisher set one (JMS/SMF clients typically do; the REST
                # messaging API used in dev testing doesn't) — fall back to
                # the broker-assigned replication-group id, which every
                # spooled message always has.
                message_id = msg.get_application_message_id() or msg.get_replication_group_message_id()
                samples.append(
                    MessageSample(
                        resource_id=resource_id,
                        message_id=str(message_id) if message_id is not None else None,
                        timestamp=msg.get_time_stamp(),
                        topic=msg.get_destination_name(),
                        headers=headers,
                        body_preview=body_preview,
                        size_bytes=size_bytes,
                    )
                )
            return samples
        finally:
            browser.terminate()
    finally:
        service.disconnect()
