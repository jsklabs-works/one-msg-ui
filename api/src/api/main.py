"""Unified API layer — see /docs/architecture.md §4.

Run with: uvicorn api.main:app --reload --port 8000
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from . import models
from .aggregator import BrokerRegistry
from .config import load_broker_configs

app = FastAPI(title="one-msg-ui API", version="0.1.0")

# Local dev only — the Vite UI dev server runs on a different origin.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_registry = BrokerRegistry(load_broker_configs())


def _find_resource(resource_id: str) -> models.Resource:
    _, resources, _, _ = _registry.fetch_all()
    for r in resources:
        if r.id == resource_id:
            return r
    raise HTTPException(status_code=404, detail=f"no such resource: {resource_id}")


@app.get("/api/brokers", response_model=list[models.Broker])
def get_brokers():
    brokers, _, _, _ = _registry.fetch_all()
    return brokers


@app.get("/api/resources", response_model=list[models.Resource])
def get_resources(
    environment: str | None = Query(default=None),
    system_type: models.SystemType | None = Query(default=None),
    broker_id: str | None = Query(default=None),
):
    brokers, resources, _, _ = _registry.fetch_all()
    env_by_broker = {b.id: b.environment for b in brokers}

    def matches(r: models.Resource) -> bool:
        if system_type and r.system_type != system_type:
            return False
        if broker_id and r.broker_id != broker_id:
            return False
        if environment and env_by_broker.get(r.broker_id) != environment:
            return False
        return True

    return [r for r in resources if matches(r)]


@app.get("/api/resources/{resource_id}", response_model=models.Resource)
def get_resource(resource_id: str):
    return _find_resource(resource_id)


@app.get("/api/health", response_model=list[models.HealthEvent])
def get_health(broker_id: str | None = Query(default=None)):
    _, _, health, _ = _registry.fetch_all()
    if broker_id:
        health = [h for h in health if h.broker_id == broker_id]
    return health


@app.get("/api/consumer-groups", response_model=list[models.ConsumerGroup])
def get_consumer_groups(broker_id: str | None = Query(default=None)):
    _, _, _, groups = _registry.fetch_all()
    if broker_id:
        groups = [g for g in groups if g.broker_id == broker_id]
    return groups


@app.get("/api/resources/{resource_id}/messages", response_model=list[models.MessageSample])
def get_resource_messages(resource_id: str, limit: int = Query(default=10, ge=1, le=100)):
    resource = _find_resource(resource_id)
    try:
        return _registry.peek(resource, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"peek failed: {exc}") from exc
