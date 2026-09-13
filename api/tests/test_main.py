from unittest.mock import patch

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


def test_import_brokers_adds_multiple_and_reports_added_status():
    client = _client_with()
    with patch.object(BrokerRegistry, "_fetch_kafka", return_value=([], [], [])):
        with patch.object(BrokerRegistry, "_fetch_solace", return_value=([], [], [])):
            with patch.object(main, "save_broker_configs") as mock_save:
                resp = client.post(
                    "/api/brokers/import",
                    json={
                        "brokers": [
                            {"type": "kafka", "name": "Imported Kafka", "environment": "dev", "config": {"bootstrap_servers": "a:9092"}},
                            {
                                "type": "solace",
                                "name": "Imported Solace",
                                "environment": "dev",
                                "config": {
                                    "semp_url": "http://localhost:8080",
                                    "smf_host": "tcp://localhost:55554",
                                    "username": "admin",
                                    "password": "admin",
                                },
                            },
                        ]
                    },
                )
    mock_save.assert_called_once()
    assert resp.status_code == 200
    results = resp.json()
    assert [r["status"] for r in results] == ["added", "added"]
    assert results[0]["broker"]["id"] == "imported-kafka"
    # Actually persisted into the registry, not just reported as added.
    assert main._registry.get_config("imported-kafka") is not None
    assert main._registry.get_config("imported-solace") is not None


def test_import_brokers_flags_a_dupe_within_the_same_file_after_the_first_is_added():
    client = _client_with()
    with patch.object(BrokerRegistry, "_fetch_solace", return_value=([], [], [])):
        with patch.object(main, "save_broker_configs"):
            resp = client.post(
                "/api/brokers/import",
                json={
                    "brokers": [
                        {
                            "type": "solace",
                            "name": "local",
                            "environment": "dev",
                            "config": {
                                "semp_url": "http://localhost:8080",
                                "smf_host": "tcp://localhost:55554",
                                "username": "admin",
                                "password": "admin",
                            },
                        },
                        {
                            "type": "solace",
                            "name": "local2",
                            "environment": "dev",
                            "config": {
                                "semp_url": "http://localhost:8080/",
                                "smf_host": "tcp://localhost:55554",
                                "username": "admin",
                                "password": "admin",
                            },
                        },
                    ]
                },
            )
    assert resp.status_code == 200
    results = resp.json()
    assert results[0]["status"] == "added"
    assert results[1]["status"] == "duplicate_connection"
    assert "local" in results[1]["detail"]
    # Only the first one actually got added.
    assert main._registry.get_config("local") is not None
    assert main._registry.get_config("local2") is None


def test_import_brokers_continues_past_a_bad_entry_and_reports_it():
    client = _client_with(
        BrokerConfig(id="existing", type="kafka", name="Existing", environment="dev", config={"bootstrap_servers": "x:9092"})
    )
    with patch.object(BrokerRegistry, "_fetch_kafka", return_value=([], [], [])):
        with patch.object(main, "save_broker_configs"):
            resp = client.post(
                "/api/brokers/import",
                json={
                    "brokers": [
                        {"type": "kafka", "name": "Existing", "environment": "dev", "config": {"bootstrap_servers": "y:9092"}},
                        {"type": "kafka", "name": "", "environment": "dev", "config": {}},
                        {"type": "kafka", "name": "Good One", "environment": "dev", "config": {"bootstrap_servers": "z:9092"}},
                    ]
                },
            )
    assert resp.status_code == 200
    statuses = [r["status"] for r in resp.json()]
    assert statuses == ["duplicate_name", "invalid", "added"]


def test_import_brokers_does_not_persist_when_nothing_was_added():
    client = _client_with(
        BrokerConfig(id="existing", type="kafka", name="Existing", environment="dev", config={"bootstrap_servers": "x:9092"})
    )
    with patch.object(main, "save_broker_configs") as mock_save:
        resp = client.post(
            "/api/brokers/import",
            json={"brokers": [{"type": "kafka", "name": "Existing", "environment": "dev", "config": {"bootstrap_servers": "y:9092"}}]},
        )
    assert resp.status_code == 200
    assert resp.json()[0]["status"] == "duplicate_name"
    mock_save.assert_not_called()
