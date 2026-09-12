from api.aggregator import _kafka_security_kwargs
from api.config import config_bool


def test_config_bool_defaults_when_absent():
    assert config_bool({}, "use_tls", False) is False
    assert config_bool({}, "use_tls", True) is True


def test_config_bool_accepts_real_bool():
    assert config_bool({"x": True}, "x", False) is True
    assert config_bool({"x": False}, "x", True) is False


def test_config_bool_accepts_truthy_and_falsy_strings():
    for truthy in ("true", "True", "1", "yes", "on"):
        assert config_bool({"x": truthy}, "x", False) is True
    for falsy in ("false", "False", "0", "no", "off", ""):
        assert config_bool({"x": falsy}, "x", True) is False


def test_kafka_security_kwargs_plaintext_by_default():
    assert _kafka_security_kwargs({})["security_protocol"] == "PLAINTEXT"


def test_kafka_security_kwargs_ssl_when_tls_only():
    kwargs = _kafka_security_kwargs({"use_tls": True})
    assert kwargs["security_protocol"] == "SSL"
    assert kwargs["sasl_mechanism"] is None


def test_kafka_security_kwargs_sasl_plaintext_when_creds_no_tls():
    kwargs = _kafka_security_kwargs({"sasl_username": "u", "sasl_password": "p"})
    assert kwargs["security_protocol"] == "SASL_PLAINTEXT"
    assert kwargs["sasl_mechanism"] == "PLAIN"
    assert kwargs["sasl_plain_username"] == "u"


def test_kafka_security_kwargs_sasl_ssl_when_both():
    kwargs = _kafka_security_kwargs({"use_tls": True, "sasl_username": "u", "sasl_password": "p"})
    assert kwargs["security_protocol"] == "SASL_SSL"


def test_kafka_security_kwargs_passes_through_ssl_cafile():
    assert _kafka_security_kwargs({"ssl_cafile": "/tmp/ca.pem"})["ssl_cafile"] == "/tmp/ca.pem"
    assert _kafka_security_kwargs({})["ssl_cafile"] is None
