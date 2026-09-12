import json

from api.config import BrokerConfig, load_broker_configs, save_broker_configs, slugify, validate_broker_config_fields


def test_validate_broker_config_fields_flags_missing_required_field():
    errors = validate_broker_config_fields("kafka", {})
    assert any("Bootstrap servers" in e for e in errors)


def test_validate_broker_config_fields_ok_when_required_fields_present():
    assert validate_broker_config_fields("kafka", {"bootstrap_servers": "localhost:9092"}) == []


def test_validate_broker_config_fields_optional_field_not_required():
    # mq's queue_pattern is optional — omitting it shouldn't be an error.
    config = {
        "admin_url": "https://localhost:9543",
        "qmgr_name": "QM1",
        "admin_username": "admin",
        "admin_password": "x",
        "app_username": "app",
        "app_password": "x",
    }
    assert validate_broker_config_fields("mq", config) == []


def test_slugify_lowercases_and_dashes():
    assert slugify("My Kafka Cluster!") == "my-kafka-cluster"


def test_slugify_falls_back_when_nothing_survives():
    assert slugify("!!!") == "broker"


def test_load_broker_configs_returns_empty_list_when_file_missing(tmp_path):
    # The empty-state scenario: a fresh install with no brokers.json yet.
    missing = tmp_path / "does-not-exist.json"
    assert load_broker_configs(missing) == []


def test_save_and_load_round_trip(tmp_path):
    path = tmp_path / "brokers.json"
    configs = [BrokerConfig(id="b1", type="kafka", name="B1", environment="dev", config={"bootstrap_servers": "x:9092"})]
    save_broker_configs(configs, path)

    loaded = load_broker_configs(path)
    assert loaded == configs


def test_save_broker_configs_writes_readable_json(tmp_path):
    path = tmp_path / "brokers.json"
    save_broker_configs([BrokerConfig(id="b1", type="kafka", name="B1", environment="dev", config={})], path)
    data = json.loads(path.read_text())
    assert data["brokers"][0]["id"] == "b1"
