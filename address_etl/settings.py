from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    sparql_endpoint: str
    esri_username: str
    esri_password: str

    sqlite_conn_str: str = "address.db"
    esri_geocode_rest_api_url: str = "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/Address_Geocodes/FeatureServer/0/query"
    esri_auth_url: str = (
        "https://qportal.information.qld.gov.au/arcgis/sharing/rest/generateToken"
    )
    esri_referer: str = "https://qportal.information.qld.gov.au/arcgis/"
    populate_geocode_table: bool = True
    http_retry_max_time_in_seconds: int = 3600
    http_timeout_in_seconds: int = 120
    address_iri_limit: int | None = None


settings = Settings()
