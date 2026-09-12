"""Broker inventory — which brokers this API instance talks to.

Per architecture.md §7, this belongs in a metadata store (Postgres) with
encrypted credentials long-term. For this MVP it's a plain JSON file —
explicitly a placeholder, not a decision to keep plaintext credentials in
a repo-tracked file for anything beyond a local dev broker. See README.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import BaseModel

from .models import SystemType

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "brokers.json"


class BrokerConfig(BaseModel):
    id: str
    type: SystemType
    name: str
    environment: str
    config: dict


def load_broker_configs(path: str | Path | None = None) -> list[BrokerConfig]:
    config_path = Path(path or os.environ.get("ONE_MSG_UI_BROKERS_CONFIG", _DEFAULT_CONFIG_PATH))
    data = json.loads(config_path.read_text())
    return [BrokerConfig.model_validate(b) for b in data["brokers"]]
