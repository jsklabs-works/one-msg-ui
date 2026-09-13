"""Broker inventory — which brokers this API instance talks to.

Per architecture.md §7, this belongs in a metadata store (Postgres) with
encrypted credentials long-term. For this MVP it's a plain JSON file that
the API now reads *and writes* (see save_broker_configs) — still an
explicit placeholder, not a decision to keep plaintext credentials in a
repo-tracked file for anything beyond local dev brokers. See README.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from .models import SystemType

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "brokers.json"


class BrokerConfig(BaseModel):
    id: str
    type: SystemType
    name: str
    environment: str
    config: dict


class FieldSpec(BaseModel):
    """One connection-config field a broker type needs. Drives the UI's
    "add broker" form dynamically — the frontend has no hardcoded
    knowledge of what Kafka vs. Solace vs. MQ need, it just renders
    whatever this describes. Adding a 4th adapter later means adding a
    FIELD_SPECS entry here, not touching the UI.
    """

    name: str
    label: str
    type: Literal["text", "password", "checkbox"]
    required: bool = True
    default: str | None = None
    placeholder: str | None = None


# One entry per adapter's real client constructor signature (verified
# against adapters/*/src/*/client.py — not guessed) plus what peek.py
# needs where that differs (MQ's app vs. admin credential separation).
# TLS/SASL fields are real, not cosmetic — every one of them is actually
# wired through in aggregator.py to the adapter constructor/peek call
# that uses it; see each adapter's client.py for what it does.
FIELD_SPECS: dict[SystemType, list[FieldSpec]] = {
    "kafka": [
        FieldSpec(name="bootstrap_servers", label="Bootstrap servers", type="text", placeholder="localhost:9092"),
        FieldSpec(name="use_tls", label="Use TLS (SSL)", type="checkbox", required=False, default="false"),
        FieldSpec(name="sasl_username", label="SASL username", type="text", required=False),
        FieldSpec(name="sasl_password", label="SASL password", type="password", required=False),
        FieldSpec(name="ssl_cafile", label="CA certificate path", type="text", required=False),
    ],
    "solace": [
        FieldSpec(name="semp_url", label="SEMP v2 URL", type="text", placeholder="http://localhost:8080"),
        FieldSpec(name="smf_host", label="SMF host (for message peek)", type="text", placeholder="tcp://localhost:55554"),
        FieldSpec(
            name="vpn_name",
            label="Message VPN (leave blank to monitor every VPN these credentials can see)",
            type="text",
            required=False,
        ),
        FieldSpec(name="username", label="Username", type="text", default="admin"),
        FieldSpec(name="password", label="Password", type="password"),
        FieldSpec(
            name="verify_certificate",
            label="Verify server certificate (uncheck for self-signed dev certs)",
            type="checkbox",
            required=False,
            default="true",
        ),
    ],
    "mq": [
        FieldSpec(name="admin_url", label="Admin/REST URL", type="text", placeholder="https://localhost:9543"),
        FieldSpec(name="qmgr_name", label="Queue manager name", type="text", placeholder="QM1"),
        FieldSpec(name="admin_username", label="Admin username", type="text", default="admin"),
        FieldSpec(name="admin_password", label="Admin password", type="password"),
        FieldSpec(name="app_username", label="App username (for message peek)", type="text", default="app"),
        FieldSpec(name="app_password", label="App password", type="password"),
        FieldSpec(name="queue_pattern", label="Queue name pattern", type="text", required=False, default="*"),
        FieldSpec(
            name="verify_tls",
            label="Verify server certificate (uncheck for self-signed dev certs)",
            type="checkbox",
            required=False,
            default="false",
        ),
    ],
}

SYSTEM_TYPE_LABELS: dict[SystemType, str] = {"kafka": "Apache Kafka", "solace": "Solace PubSub+", "mq": "IBM MQ"}


def validate_broker_config_fields(system_type: SystemType, config: dict) -> list[str]:
    """Returns a list of human-readable errors — empty means valid."""
    errors = []
    for spec in FIELD_SPECS[system_type]:
        if spec.required and not config.get(spec.name):
            errors.append(f"{spec.label} is required")
    return errors


def connection_identity(system_type: SystemType, config: dict) -> tuple[str, ...]:
    """A normalized key identifying which physical broker/endpoint a
    connection actually points at — used to reject a second broker aimed
    at the same place under a different display name (see
    aggregator.BrokerRegistry.find_duplicate). Deliberately narrower than
    "identical config": different credentials or a different Solace VPN
    filter can still be a legitimate second connection; the same admin
    endpoint for the same thing is always a duplicate no matter what it's
    named.
    """
    if system_type == "kafka":
        return (config.get("bootstrap_servers", "").strip().lower(),)
    if system_type == "solace":
        return (config.get("semp_url", "").strip().lower().rstrip("/"),)
    if system_type == "mq":
        return (
            config.get("admin_url", "").strip().lower().rstrip("/"),
            config.get("qmgr_name", "").strip(),
        )
    return ()


def config_bool(config: dict, key: str, default: bool) -> bool:
    """Broker `config` values come from JSON (already real bools if the
    UI sent them that way) or from CLI/manual edits (often "true"/"false"
    strings) — accept both rather than silently misreading a string as
    truthy.
    """
    value = config.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "broker"


def _config_path(path: str | Path | None = None) -> Path:
    return Path(path or os.environ.get("ONE_MSG_UI_BROKERS_CONFIG", _DEFAULT_CONFIG_PATH))


def load_broker_configs(path: str | Path | None = None) -> list[BrokerConfig]:
    config_path = _config_path(path)
    if not config_path.exists():
        return []
    data = json.loads(config_path.read_text())
    return [BrokerConfig.model_validate(b) for b in data.get("brokers", [])]


def save_broker_configs(configs: list[BrokerConfig], path: str | Path | None = None) -> None:
    config_path = _config_path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps({"brokers": [c.model_dump() for c in configs]}, indent=2) + "\n")
