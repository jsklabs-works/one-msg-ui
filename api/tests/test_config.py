import json

from api.config import (
    BrokerConfig,
    connection_identity,
    load_broker_configs,
    save_broker_configs,
    slugify,
    validate_broker_config_fields,
)


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


def test_connection_identity_kafka_normalizes_case_and_whitespace():
    a = connection_identity("kafka", {"bootstrap_servers": "  Localhost:9092  "})
    b = connection_identity("kafka", {"bootstrap_servers": "localhost:9092"})
    assert a == b


def test_connection_identity_kafka_distinguishes_different_bootstrap_servers():
    a = connection_identity("kafka", {"bootstrap_servers": "host1:9092"})
    b = connection_identity("kafka", {"bootstrap_servers": "host2:9092"})
    assert a != b


def test_connection_identity_solace_ignores_trailing_slash_and_case():
    a = connection_identity("solace", {"semp_url": "HTTP://Localhost:8080/"})
    b = connection_identity("solace", {"semp_url": "http://localhost:8080"})
    assert a == b


def test_connection_identity_solace_ignores_vpn_and_credentials():
    # Deliberate: a different vpn_name/username/password is a legitimate
    # second connection to the same SEMP endpoint, not a duplicate — see
    # connection_identity's docstring.
    a = connection_identity("solace", {"semp_url": "http://localhost:8080", "vpn_name": "vpn-a", "username": "u1"})
    b = connection_identity("solace", {"semp_url": "http://localhost:8080", "vpn_name": "vpn-b", "username": "u2"})
    assert a == b


def test_connection_identity_mq_requires_both_admin_url_and_qmgr_name_to_match():
    same_qmgr = connection_identity("mq", {"admin_url": "https://localhost:9543", "qmgr_name": "QM1"})
    same_qmgr_2 = connection_identity("mq", {"admin_url": "https://localhost:9543/", "qmgr_name": "QM1"})
    diff_qmgr = connection_identity("mq", {"admin_url": "https://localhost:9543", "qmgr_name": "QM2"})
    assert same_qmgr == same_qmgr_2
    assert same_qmgr != diff_qmgr


def test_connection_identity_unknown_type_returns_empty_tuple():
    assert connection_identity("bogus", {"anything": "x"}) == ()
