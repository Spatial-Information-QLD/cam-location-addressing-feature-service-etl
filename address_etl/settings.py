from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    sparql_endpoint: str
    esri_username: str
    esri_password: str

    sqlite_conn_str: str = "address.db"
    pls_sqlite_conn_str: str = "pls.db"

    # Location Addressing
    esri_location_addressing_rest_api_query_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/Queensland_Location_Address_Maintenance_UAT/FeatureServer/0/query"
    )
    esri_location_addressing_rest_api_apply_edit_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/Queensland_Location_Address_Maintenance_UAT/FeatureServer/0/applyEdits"
    )

    # Geocodes
    esri_geocode_rest_api_query_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/Address_Geocodes_UAT/FeatureServer/0/query"
    )
    esri_geocode_rest_api_apply_edit_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/Address_Geocodes_UAT/FeatureServer/0/applyEdits"
    )

    # PLS local_auth
    esri_pls_local_auth_api_query_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/7/query"
    )
    esri_pls_local_auth_api_apply_edit_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/7/applyEdits"
    )

    # PLS locality
    esri_pls_locality_api_query_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/8/query"
    )
    esri_pls_locality_api_apply_edit_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/8/applyEdits"
    )

    # PLS road
    esri_pls_road_api_query_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/5/query"
    )
    esri_pls_road_api_apply_edit_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/5/applyEdits"
    )

    # PLS parcel
    esri_pls_parcel_api_query_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/3/query"
    )
    esri_pls_parcel_api_apply_edit_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/3/applyEdits"
    )

    # PLS site
    esri_pls_site_api_query_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/6/query"
    )
    esri_pls_site_api_apply_edit_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/6/applyEdits"
    )

    # PLS address
    esri_pls_address_api_query_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/0/query"
    )
    esri_pls_address_api_apply_edit_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/0/applyEdits"
    )

    # PLS geocode
    esri_pls_geocode_api_query_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/1/query"
    )
    esri_pls_geocode_api_apply_edit_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/PLI_Tables_UAT/FeatureServer/1/applyEdits"
    )

    # Auth
    esri_auth_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/sharing/rest/generateToken"
    )
    esri_referer: str = "https://qportal.information.qld.gov.au/arcgis/"

    populate_geocode_table: bool = True
    http_retry_max_time_in_seconds: int = 900
    http_timeout_in_seconds: int = 600
    debug: bool = False

    # pytz
    timezone: str = "Australia/Brisbane"

    # S3
    s3_bucket_name: str = "location-addressing-feature-service-etl"
    pls_s3_bucket_name: str = "pls-feature-service-etl"
    s3_presigned_url_expiry_seconds: int = 3600

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
