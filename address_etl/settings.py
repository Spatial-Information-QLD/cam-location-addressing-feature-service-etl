from typing import Any

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    sparql_endpoint: str
    geocode_type_sparql_endpoint: str = (
        "https://icsm-api.qlocation.information.qld.gov.au/sparql"
    )
    esri_username: str
    esri_password: str

    pls_sqlite_conn_str: str = "pls.db"

    esri_geocode_rest_api_query_url: str = "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/Address_Geocodes_UAT/FeatureServer/0/query"
    esri_address_iri_pid_map_query_url: str = "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/Address_IRI_to_PID_UAT/FeatureServer/0/query"

    esri_auth_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/sharing/rest/generateToken"
    )
    esri_referer: str = "https://qportal.information.qld.gov.au/arcgis/"

    http_retry_max_time_in_seconds: int = 900
    http_timeout_in_seconds: int = 600
    debug: bool = False

    timezone: str = "Australia/Brisbane"

    pls_s3_bucket_name: str = "pls-feature-service-etl"
    s3_presigned_url_expiry_seconds: int = 3600

    kafka_topic: str
    kafka_bootstrap_server: str = "localhost:9092"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None

    # Minio S3 settings used only for testing.
    # Application code assumes role when running in AWS.
    use_minio: bool = False
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_region: str = "us-east-1"

    lock_table_name: str = "address-etl-lock"

    @model_validator(mode="after")
    def validate_kafka_sasl_settings(self) -> "Settings":
        uses_sasl = self.kafka_security_protocol.startswith("SASL")
        if not uses_sasl:
            return self

        missing = [
            field_name
            for field_name, value in (
                ("kafka_sasl_mechanism", self.kafka_sasl_mechanism),
                ("kafka_sasl_username", self.kafka_sasl_username),
                ("kafka_sasl_password", self.kafka_sasl_password),
            )
            if value in (None, "")
        ]

        if missing:
            missing_fields = ", ".join(missing)
            raise ValueError(
                "Kafka SASL configuration is incomplete. Missing: "
                f"{missing_fields}"
            )

        return self

    def kafka_client_config(self, *, client_id: str | None = None) -> dict[str, Any]:
        config: dict[str, Any] = {
            "bootstrap.servers": self.kafka_bootstrap_server,
        }

        if client_id is not None:
            config["client.id"] = client_id

        if self.kafka_security_protocol != "":
            config["security.protocol"] = self.kafka_security_protocol

        if self.kafka_security_protocol.startswith("SASL"):
            config["sasl.mechanism"] = self.kafka_sasl_mechanism
            config["sasl.username"] = self.kafka_sasl_username
            config["sasl.password"] = self.kafka_sasl_password

        return config


settings = Settings()
