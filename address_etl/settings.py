from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    sparql_endpoint: str
    esri_username: str
    esri_password: str

    sqlite_conn_str: str = "address.db"
    esri_location_addressing_rest_api_query_url: str = (
        "https://uat-qportal.information.qld.gov.au/arcgis/rest/services/LOC/Queensland_Location_Address_maintenance_DEV/FeatureServer/0/query"
    )
    esri_location_addressing_rest_api_apply_edit_url: str = (
        "https://uat-qportal.information.qld.gov.au/arcgis/rest/services/LOC/Queensland_Location_Address_maintenance_DEV/FeatureServer/0/applyEdits"
    )
    esri_geocode_rest_api_query_url: str = (
        "https://uat-qportal.information.qld.gov.au/arcgis/rest/services/LOC/Address_Geocodes/FeatureServer/0/query"
    )
    esri_geocode_rest_api_apply_edit_url: str = (
        "https://uat-qportal.information.qld.gov.au/arcgis/rest/services/LOC/Address_Geocodes/FeatureServer/0/applyEdits"
    )
    esri_auth_url: str = (
        "https://uat-qportal.information.qld.gov.au/arcgis/sharing/rest/generateToken"
    )
    esri_referer: str = "https://uat-qportal.information.qld.gov.au/arcgis/"
    populate_geocode_table: bool = True
    http_retry_max_time_in_seconds: int = 3600
    http_timeout_in_seconds: int = 120
    address_iri_limit: int | None = None
    geocode_debug: bool = False
    geocode_use_previous_result: bool = False

    # pytz
    timezone: str = "Australia/Brisbane"

    # S3
    s3_bucket_name: str = "location-addressing-feature-service-etl"

    # Minio S3 settings used only for testing
    # Application code assumes role when running in AWS
    use_minio: bool = False
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_region: str = "us-east-1"

    # Dynamodb ETL lock
    lock_table_name: str = "address-etl-lock"


settings = Settings()
