from fastapi.testclient import TestClient

from api import main
from api.aggregator import BrokerRegistry
from api.config import BrokerConfig


def _client_with(*configs: BrokerConfig) -> TestClient:
    # Swap the module-level registry for a known, isolated one rather than
    # touching the real config/brokers.json or its on-disk brokers.
    main._registry = BrokerRegistry(list(configs))
    return TestClient(main.app)


def test_create_broker_rejects_duplicate_name():
    client = _client_with(BrokerConfig(id="local-kafka", type="kafka", name="Local Kafka", environment="dev", config={"bootstrap_servers": "a:9092"}))
    resp = client.post(
        "/api/brokers",
        json={"type": "kafka", "name": "Local Kafka", "environment": "dev", "config": {"bootstrap_servers": "b:9092"}},
    )
    assert resp.status_code == 409
    assert "already exists" in resp.json()["detail"]


def test_create_broker_rejects_same_connection_under_a_different_name():
    client = _client_with(
        BrokerConfig(
            id="local-solace",
            type="solace",
            name="Local Solace",
            environment="dev",
            config={"semp_url": "http://localhost:8080"},
        )
    )
    resp = client.post(
        "/api/brokers",
        json={
            "type": "solace",
            "name": "Second Solace",
            "environment": "dev",
            "config": {
                "semp_url": "http://localhost:8080/",
                "smf_host": "tcp://localhost:55554",
                "username": "admin",
                "password": "admin",
            },
        },
    )
    assert resp.status_code == 409
    assert "already points at this same connection" in resp.json()["detail"]
    assert "Local Solace" in resp.json()["detail"]


def test_create_broker_allows_same_type_different_connection_and_reports_bad_config_as_422():
    client = _client_with(
        BrokerConfig(
            id="local-solace",
            type="solace",
            name="Local Solace",
            environment="dev",
            config={"semp_url": "http://localhost:8080"},
        )
    )
    resp = client.post(
        "/api/brokers",
        json={
            "type": "solace",
            "name": "Other Solace",
            "environment": "dev",
            "config": {
                "semp_url": "http://otherhost:8080",
                "smf_host": "tcp://otherhost:55554",
                "username": "admin",
                "password": "admin",
            },
        },
    )
    # Not a duplicate — it should get past both 409 checks and only fail
    # because there's no real broker at otherhost:8080 to connect to.
    assert resp.status_code == 422
    assert "couldn't connect" in resp.json()["detail"]
