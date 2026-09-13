"""Unified API layer — see /docs/architecture.md §4.

Run with: uvicorn api.main:app --reload --port 8000
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from . import models
from .aggregator import BrokerRegistry
from .config import (
    FIELD_SPECS,
    SYSTEM_TYPE_LABELS,
    BrokerConfig,
    FieldSpec,
    load_broker_configs,
    save_broker_configs,
    slugify,
    validate_broker_config_fields,
)

app = FastAPI(title="one-msg-ui API", version="0.1.0")

# Local dev only — the Vite UI dev server runs on a different origin.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)

_registry = BrokerRegistry(load_broker_configs())


def _find_resource(resource_id: str) -> models.Resource:
    _, resources, _, _ = _registry.fetch_all()
    for r in resources:
        if r.id == resource_id:
            return r
    raise HTTPException(status_code=404, detail=f"no such resource: {resource_id}")


class SystemTypeInfo(BaseModel):
    type: models.SystemType
    label: str
    fields: list[FieldSpec]


class CreateBrokerRequest(BaseModel):
    type: models.SystemType
    name: str
    environment: str
    config: dict


class BrokerImportResult(BaseModel):
    """One entry's outcome from POST /api/brokers/import — see that
    endpoint's docstring for why this is a per-entry report rather than
    an all-or-nothing batch.
    """

    name: str
    type: models.SystemType
    status: Literal["added", "duplicate_name", "duplicate_connection", "invalid", "connection_failed"]
    detail: str
    broker: models.Broker | None = None


class ImportBrokersRequest(BaseModel):
    # Deliberately the same shape as config/brokers.json's top-level
    # object (extra fields like a file's own "id" are just ignored —
    # pydantic v2's default) so that file can be uploaded as-is.
    brokers: list[CreateBrokerRequest]


class ExportBrokersResponse(BaseModel):
    # The export/import round trip: this is exactly config/brokers.json's
    # own on-disk shape (see save_broker_configs), so a file downloaded
    # from here can be fed straight back into POST /api/brokers/import,
    # or dropped in as another instance's config/brokers.json, unchanged.
    brokers: list[BrokerConfig]


@app.get("/api/system-types", response_model=list[SystemTypeInfo])
def get_system_types():
    """Drives the "add broker" form's system-type dropdown and, once
    chosen, its field list — the frontend has no hardcoded knowledge of
    what each broker type needs, it just renders this.
    """
    return [
        SystemTypeInfo(type=t, label=SYSTEM_TYPE_LABELS[t], fields=FIELD_SPECS[t])
        for t in ("kafka", "solace", "mq", "rabbitmq", "activemq")
    ]


@app.get("/api/brokers", response_model=list[models.Broker])
def get_brokers():
    brokers, _, _, _ = _registry.fetch_all()
    return brokers


@app.get("/api/brokers/export", response_model=ExportBrokersResponse)
def export_brokers():
    """The other half of the import round trip (POST /api/brokers/import)
    — every configured broker, full connection config included, in
    exactly config/brokers.json's own shape. That means credentials
    included in plaintext: not new exposure (this instance's
    config/brokers.json already holds them — see config.py's module
    docstring), but the UI labels the download accordingly since a
    person can now carry that file around.
    """
    return ExportBrokersResponse(brokers=_registry.broker_configs)


def _add_one_broker(req: CreateBrokerRequest) -> BrokerImportResult:
    """The actual add-a-broker logic, shared by the single POST /api/brokers
    below and the bulk importer — same checks, same order (cheapest/local
    checks before ever touching the network), in both places. Returns a
    result rather than raising, so a bulk import of N brokers can report
    "M added, the rest skipped and why" instead of an all-or-nothing
    HTTPException on the first bad entry. Does NOT call
    save_broker_configs — callers persist once, after all their entries
    are done, so an import of 20 brokers doesn't rewrite the file 20 times.
    """
    field_errors = validate_broker_config_fields(req.type, req.config)
    if field_errors:
        return BrokerImportResult(name=req.name, type=req.type, status="invalid", detail="; ".join(field_errors))

    broker_id = slugify(req.name)
    if _registry.get_config(broker_id) is not None:
        return BrokerImportResult(
            name=req.name, type=req.type, status="duplicate_name", detail=f"a broker named {req.name!r} already exists"
        )

    duplicate = _registry.find_duplicate(req.type, req.config)
    if duplicate is not None:
        return BrokerImportResult(
            name=req.name,
            type=req.type,
            status="duplicate_connection",
            detail=f"{SYSTEM_TYPE_LABELS[req.type]} broker {duplicate.name!r} already points at this same connection",
        )

    bc = BrokerConfig(id=broker_id, type=req.type, name=req.name, environment=req.environment, config=req.config)

    # Fail loudly now rather than silently later — see test_connection's
    # docstring. Nothing is persisted if this doesn't work.
    error = _registry.test_connection(bc)
    if error is not None:
        return BrokerImportResult(name=req.name, type=req.type, status="connection_failed", detail=f"couldn't connect: {error}")

    _registry.add_broker(bc)
    broker = models.Broker(id=bc.id, system_type=bc.type, name=bc.name, environment=bc.environment, status="up")
    return BrokerImportResult(name=req.name, type=req.type, status="added", detail="added", broker=broker)


_IMPORT_STATUS_TO_HTTP_CODE = {
    "invalid": 400,
    "duplicate_name": 409,
    "duplicate_connection": 409,
    "connection_failed": 422,
}


@app.post("/api/brokers", response_model=models.Broker, status_code=201)
def create_broker(req: CreateBrokerRequest):
    result = _add_one_broker(req)
    if result.status != "added":
        raise HTTPException(status_code=_IMPORT_STATUS_TO_HTTP_CODE[result.status], detail=result.detail)
    save_broker_configs(_registry.broker_configs)
    return result.broker


@app.post("/api/brokers/import", response_model=list[BrokerImportResult])
def import_brokers(req: ImportBrokersRequest):
    """Bulk version of POST /api/brokers — add several brokers at once
    from a brokers.json-shaped file instead of the add-broker form once
    per broker. Each entry gets exactly the same validation, duplicate-
    name, duplicate-connection, and connection-test checks as a single
    add (see _add_one_broker). Always returns 200 with one result per
    entry rather than failing the whole request on the first bad one —
    the UI renders per-entry status so a mostly-good file still gets
    (say) 8 of 10 brokers added instead of zero. Entries are processed
    in order and checked against each other too, so two entries in the
    same file pointing at the same connection correctly leaves only the
    first one added.
    """
    results = [_add_one_broker(entry) for entry in req.brokers]
    if any(r.status == "added" for r in results):
        save_broker_configs(_registry.broker_configs)
    return results


@app.delete("/api/brokers/{broker_id}", status_code=204)
def delete_broker(broker_id: str):
    try:
        _registry.remove_broker(broker_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"no such broker: {broker_id}")
    save_broker_configs(_registry.broker_configs)


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
        # Raw detail, no "peek failed:" prefix here — the UI already frames
        # this under "Messages (non-destructive peek)" and adds its own
        # label; doubling it up read as "Peek failed: peek failed: ...".
        raise HTTPException(status_code=502, detail=str(exc)) from exc


class PeekWithCredentials(BaseModel):
    limit: int = 10
    username: str
    password: str


@app.post("/api/resources/{resource_id}/messages", response_model=list[models.MessageSample])
def post_resource_messages(resource_id: str, body: PeekWithCredentials):
    """Retry a peek with different credentials for just this call — a
    password has no business in a URL query string, hence POST rather than
    a `?username=&password=` on the GET above. Used when a VPN/queue needs
    credentials the saved broker config doesn't have (see aggregator.peek's
    docstring); nothing here is persisted to the broker config.
    """
    resource = _find_resource(resource_id)
    try:
        return _registry.peek(resource, limit=body.limit, override_username=body.username, override_password=body.password)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
