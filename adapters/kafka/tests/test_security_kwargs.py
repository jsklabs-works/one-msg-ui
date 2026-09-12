from kafka_adapter.client import security_kwargs


def test_plaintext_default():
    assert security_kwargs() == {"security_protocol": "PLAINTEXT"}


def test_ssl_without_sasl():
    kwargs = security_kwargs(security_protocol="SSL")
    assert kwargs == {"security_protocol": "SSL"}


def test_sasl_adds_credentials():
    kwargs = security_kwargs(
        security_protocol="SASL_SSL", sasl_mechanism="PLAIN", sasl_plain_username="u", sasl_plain_password="p"
    )
    assert kwargs == {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": "u",
        "sasl_plain_password": "p",
    }


def test_ssl_cafile_included_only_when_given():
    assert "ssl_cafile" not in security_kwargs()
    assert security_kwargs(ssl_cafile="/tmp/ca.pem")["ssl_cafile"] == "/tmp/ca.pem"


def test_no_sasl_mechanism_means_no_sasl_credentials_even_if_provided():
    # Defensive: a caller passing a username without a mechanism shouldn't
    # silently produce a half-configured SASL setup.
    kwargs = security_kwargs(sasl_plain_username="u", sasl_plain_password="p")
    assert "sasl_plain_username" not in kwargs
