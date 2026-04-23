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

    # Minio S3 settings used only for testing.
    # Application code assumes role when running in AWS.
    use_minio: bool = False
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_region: str = "us-east-1"

    lock_table_name: str = "address-etl-lock"


settings = Settings()
