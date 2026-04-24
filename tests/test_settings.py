import pytest
from pydantic import ValidationError

from address_etl.settings import Settings


def build_settings(**overrides) -> Settings:
    defaults = {
        "sparql_endpoint": "http://mock-sparql",
        "esri_username": "mock-user",
        "esri_password": "mock-pass",
        "kafka_topic": "pls.artifact-url.v1",
    }
    defaults.update(overrides)
    return Settings(_env_file=None, **defaults)


def test_settings_build_plaintext_kafka_client_config():
    settings = build_settings(
        kafka_bootstrap_server="localhost:9092",
        kafka_security_protocol="PLAINTEXT",
    )

    assert settings.kafka_client_config(client_id="pls-etl-test") == {
        "bootstrap.servers": "localhost:9092",
        "client.id": "pls-etl-test",
        "security.protocol": "PLAINTEXT",
    }


def test_settings_build_scram_kafka_client_config():
    settings = build_settings(
        kafka_bootstrap_server="b-1.example.kafka.amazonaws.com:9096",
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="SCRAM-SHA-512",
        kafka_sasl_username="msk-user",
        kafka_sasl_password="msk-password",
    )

    assert settings.kafka_client_config(client_id="pls-etl-test") == {
        "bootstrap.servers": "b-1.example.kafka.amazonaws.com:9096",
        "client.id": "pls-etl-test",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "msk-user",
        "sasl.password": "msk-password",
    }


def test_settings_require_sasl_credentials_for_sasl_protocols():
    with pytest.raises(ValidationError, match="Kafka SASL configuration is incomplete"):
        build_settings(
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism=None,
            kafka_sasl_username="msk-user",
            kafka_sasl_password=None,
        )
