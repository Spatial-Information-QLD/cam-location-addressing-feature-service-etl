import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import backoff
import httpx
from jinja2 import Template
from rich.progress import track

from address_etl.esri_rest_api import get_count, get_esri_token
from address_etl.settings import settings
from address_etl.time_convert import datetime_to_esri_datetime_utc

logger = logging.getLogger(__name__)

GEOCODE_TYPE_URI_PREFIX = "https://linked.data.gov.au/def/geocode-types/"


@dataclass(frozen=True)
class GeocodeLayerSchema:
    object_id_field: str
    address_pid_field: str
    geocode_type_field: str
    geocode_source_field: str | None
    geocode_status_field: str | None
    last_edited_field: str | None

    @property
    def supports_incremental_import(self) -> bool:
        return self.last_edited_field is not None


def on_backoff_handler(details):
    """Handler for backoff errors"""
    logger.warning(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


def build_geocode_type_code_query(geocode_type_iris: list[str]) -> str:
    return Template(
        """
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?type ?code
        WHERE {
          VALUES ?type {
            {% for geocode_type_iri in geocode_type_iris %}
            <{{ geocode_type_iri }}>
            {% endfor %}
          }
          ?type skos:inScheme <https://linked.data.gov.au/def/geocode-types> ;
                skos:notation ?code .
          FILTER(datatype(?code) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
        }
        LIMIT 1000
        """
    ).render(geocode_type_iris=geocode_type_iris)


@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPError,),
    max_time=settings.http_retry_max_time_in_seconds,
    on_backoff=on_backoff_handler,
)
def sparql_values_query(
    sparql_endpoint: str,
    query: str,
    client: httpx.Client,
) -> httpx.Response:
    response = client.post(
        sparql_endpoint,
        headers={"Accept": "application/sparql-results+json"},
        data={"query": query},
    )
    response.raise_for_status()
    return response


def parse_geocode_type_code_bindings(payload: dict[str, Any]) -> dict[str, str]:
    return {
        binding["type"]["value"]: binding["code"]["value"]
        for binding in payload["results"]["bindings"]
    }


def load_geocode_type_codes(cursor: sqlite3.Cursor) -> dict[str, str]:
    cursor.execute(
        """
        SELECT geocode_type_iri, geocode_type_code
        FROM geocode_type_code
        """
    )
    return {
        row["geocode_type_iri"]: row["geocode_type_code"]
        for row in cursor.fetchall()
    }


def save_geocode_type_codes(
    cursor: sqlite3.Cursor,
    geocode_type_codes: dict[str, str],
) -> None:
    if not geocode_type_codes:
        return

    cursor.executemany(
        """
        INSERT INTO geocode_type_code (geocode_type_iri, geocode_type_code)
        VALUES (?, ?)
        ON CONFLICT(geocode_type_iri)
        DO UPDATE SET geocode_type_code = excluded.geocode_type_code
        """,
        sorted(geocode_type_codes.items()),
    )
    cursor.connection.commit()


def normalize_geocode_type(
    value: Any,
    geocode_type_codes: dict[str, str] | None = None,
) -> str | None:
    """Convert live geocode type values into the short legacy codes used downstream."""
    if value is None:
        return None

    geocode_type = str(value).strip()
    if not geocode_type:
        return None

    if geocode_type.startswith(GEOCODE_TYPE_URI_PREFIX):
        if geocode_type_codes and geocode_type in geocode_type_codes:
            return geocode_type_codes[geocode_type]

        slug = geocode_type.removeprefix(GEOCODE_TYPE_URI_PREFIX)

        # Fallback for newly-added geocode types that do not have an explicit
        # SIR-PUB notation available yet.
        return "".join(part[0].upper() for part in slug.split("-") if part)[:4]

    return geocode_type


def get_geocode_layer_schema(layer_definition: dict[str, Any]) -> GeocodeLayerSchema:
    field_names = {field["name"] for field in layer_definition["fields"]}
    object_id_field = (
        layer_definition.get("objectIdField")
        or layer_definition.get("objectIdFieldName")
        or "objectid"
    )

    if "address_pid" in field_names:
        address_pid_field = "address_pid"
    elif "pid" in field_names:
        address_pid_field = "pid"
    else:
        raise RuntimeError("Geocode layer schema is missing address PID field")

    if "geocode_type" in field_names:
        geocode_type_field = "geocode_type"
    elif "type" in field_names:
        geocode_type_field = "type"
    else:
        raise RuntimeError("Geocode layer schema is missing geocode type field")

    geocode_source_field = None
    if "geocode_source" in field_names:
        geocode_source_field = "geocode_source"
    elif "source" in field_names:
        geocode_source_field = "source"

    return GeocodeLayerSchema(
        object_id_field=object_id_field,
        address_pid_field=address_pid_field,
        geocode_type_field=geocode_type_field,
        geocode_source_field=geocode_source_field,
        geocode_status_field="geocode_status" if "geocode_status" in field_names else None,
        last_edited_field="last_edited_date" if "last_edited_date" in field_names else None,
    )


def build_geocode_where_clause(
    schema: GeocodeLayerSchema,
    esri_date: str | None,
) -> str:
    if esri_date:
        if schema.last_edited_field:
            return f"{schema.last_edited_field} >= DATE '{esri_date}'"
        else:
            logger.warning(
                "Geocode layer no longer exposes last_edited_date; falling back to a full geocode refresh"
            )

    return "1=1"


def normalize_geocode_feature(
    feature: dict[str, Any],
    schema: GeocodeLayerSchema,
    geocode_type_codes: dict[str, str] | None = None,
) -> dict[str, Any]:
    attrs = feature["attributes"]

    return {
        "attributes": {
            "objectid": str(attrs[schema.object_id_field]),
            "address_pid": str(attrs[schema.address_pid_field]),
            "geocode_type": normalize_geocode_type(
                attrs[schema.geocode_type_field],
                geocode_type_codes,
            ),
        },
        "geometry": feature["geometry"],
    }


def get_layer_url(query_url: str) -> str:
    if query_url.endswith("/query"):
        return query_url.removesuffix("/query")
    return query_url


def insert_geocodes(cursor: sqlite3.Cursor, features: list[dict[str, Any]]):
    """Insert geocodes into the database or update existing rows"""
    for feature in features:
        attrs = feature["attributes"]
        geom = feature["geometry"]

        cursor.execute(
            "SELECT * FROM geocode WHERE geocode_id = ?",
            (attrs["objectid"],),
        )

        if cursor.fetchone():
            cursor.execute(
                """
                UPDATE geocode SET geocode_type = ?, address_pid = ?, longitude = ?, latitude = ? WHERE geocode_id = ?
                """,
                (
                    attrs["geocode_type"],
                    attrs["address_pid"],
                    geom["x"],
                    geom["y"],
                    attrs["objectid"],
                ),
            )
        else:
            cursor.execute(
                """
                INSERT INTO geocode (geocode_id, geocode_type, address_pid, longitude, latitude)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    attrs["objectid"],
                    attrs["geocode_type"],
                    attrs["address_pid"],
                    geom["x"],
                    geom["y"],
                ),
            )


def insert_geocodes_pls(cursor: sqlite3.Cursor, features: list[dict[str, Any]]):
    """Insert geocodes into the PLS database"""
    for feature in features:
        attrs = feature["attributes"]
        geom = feature["geometry"]

        cursor.execute(
            "SELECT * FROM lf_geocode_sp_survey_point WHERE geocode_id = ?",
            (attrs["objectid"],),
        )

        if cursor.fetchone():
            cursor.execute(
                """
                UPDATE lf_geocode_sp_survey_point SET geocode_type = ?, address_pid = ?, site_id = ?, centoid_lat = ?, centoid_lon = ? WHERE geocode_id = ?
                """,
                (
                    attrs["geocode_type"],
                    attrs["address_pid"],
                    None,
                    geom["y"],
                    geom["x"],
                    attrs["objectid"],
                ),
            )
        else:
            cursor.execute(
                """
                INSERT INTO lf_geocode_sp_survey_point (geocode_id, geocode_type, address_pid, site_id, centoid_lat, centoid_lon)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    attrs["objectid"],
                    attrs["geocode_type"],
                    attrs["address_pid"],
                    None,
                    geom["y"],
                    geom["x"],
                ),
            )


class GeocodeImporter:
    def __init__(
        self,
        cursor: sqlite3.Cursor,
        client: httpx.Client,
        esri_date: str | None = None,
    ) -> None:
        self.cursor = cursor
        self.client = client
        self.access_token = get_esri_token(
            settings.esri_auth_url,
            settings.esri_referer,
            settings.esri_username,
            settings.esri_password,
            client,
        )
        self.schema = self.fetch_layer_schema()
        self.where_clause = build_geocode_where_clause(self.schema, esri_date)
        self.requires_full_refresh = bool(
            esri_date and not self.schema.supports_incremental_import
        )
        self.geocode_type_codes = self.fetch_geocode_type_codes()

        self.geocode_count = get_count(
            self.where_clause,
            settings.esri_geocode_rest_api_query_url,
            self.client,
            self.access_token,
        )

    def fetch_layer_schema(self) -> GeocodeLayerSchema:
        params = {"f": "json", "token": self.access_token}
        response = self.client.get(
            get_layer_url(settings.esri_geocode_rest_api_query_url),
            params=params,
        )
        response.raise_for_status()
        payload = response.json()

        if "error" in payload:
            error = payload["error"]
            raise RuntimeError(
                f"Error fetching geocode layer schema: ESRI error {error.get('code')}: {error.get('message')}"
            )

        if "fields" not in payload:
            raise RuntimeError(
                f"Error fetching geocode layer schema: expected 'fields' in response, got: {response.text[:1000]}"
            )

        return get_geocode_layer_schema(payload)

    def fetch_geocode_type_codes(self) -> dict[str, str]:
        if self.schema.geocode_type_field == "geocode_type":
            return {}

        stored_geocode_type_codes = load_geocode_type_codes(self.cursor)

        params = {
            "where": f"{self.schema.geocode_type_field} IS NOT NULL",
            "outFields": self.schema.geocode_type_field,
            "returnDistinctValues": "true",
            "returnGeometry": "false",
            "token": self.access_token,
            "f": "json",
        }
        response = self.client.get(
            settings.esri_geocode_rest_api_query_url,
            params=params,
        )
        response.raise_for_status()
        payload = response.json()

        if "error" in payload:
            error = payload["error"]
            raise RuntimeError(
                f"Error fetching distinct geocode types: ESRI error {error.get('code')}: {error.get('message')}"
            )

        geocode_type_iris = sorted(
            {
                feature["attributes"][self.schema.geocode_type_field]
                for feature in payload.get("features", [])
                if feature["attributes"].get(self.schema.geocode_type_field, "").startswith(
                    GEOCODE_TYPE_URI_PREFIX
                )
            }
        )

        if not geocode_type_iris:
            return stored_geocode_type_codes

        missing_geocode_type_iris = [
            geocode_type_iri
            for geocode_type_iri in geocode_type_iris
            if geocode_type_iri not in stored_geocode_type_codes
        ]

        if not missing_geocode_type_iris:
            return stored_geocode_type_codes

        query = build_geocode_type_code_query(missing_geocode_type_iris)
        try:
            response = sparql_values_query(
                settings.geocode_type_sparql_endpoint,
                query,
                self.client,
            )
            payload = response.json()
            fetched_geocode_type_codes = parse_geocode_type_code_bindings(payload)
            if fetched_geocode_type_codes:
                save_geocode_type_codes(
                    self.cursor,
                    fetched_geocode_type_codes,
                )
            return stored_geocode_type_codes | fetched_geocode_type_codes
        except httpx.HTTPError as error:
            if stored_geocode_type_codes:
                logger.warning(
                    "Failed to refresh geocode type codes from SPARQL; using saved values where available: %s",
                    error,
                )
                return stored_geocode_type_codes
            raise

    def import_geocodes(self, is_pls: bool = False) -> None:
        logger.info(f"Fetching {self.geocode_count} geocodes")
        batch_size = 2000
        for offset in track(
            range(0, self.geocode_count, batch_size),
            description="Processing geocodes",
        ):
            features = self.fetch_geocodes(offset, batch_size)
            if not features:
                logger.warning(f"No geocodes found for offset {offset}")
            if is_pls:
                insert_geocodes_pls(self.cursor, features)
            else:
                insert_geocodes(self.cursor, features)
            self.cursor.connection.commit()

    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPError, KeyError),
        max_time=settings.http_retry_max_time_in_seconds,
        on_backoff=on_backoff_handler,
    )
    def fetch_geocodes(self, offset: int, batch_size: int) -> list[dict[str, Any]]:
        """Fetch a batch of geocodes from the service"""
        params = {
            "where": self.where_clause,
            "outFields": ",".join(
                (
                    self.schema.object_id_field,
                    self.schema.geocode_type_field,
                    self.schema.address_pid_field,
                )
            ),
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "token": self.access_token,
            "f": "json",
        }
        logger.info(
            f"Fetching {batch_size} geocodes from {offset} of total {self.geocode_count}"
        )
        response = self.client.get(
            settings.esri_geocode_rest_api_query_url, params=params
        )
        response.raise_for_status()
        data = response.json()

        try:
            return [
                normalize_geocode_feature(
                    feature,
                    self.schema,
                    self.geocode_type_codes,
                )
                for feature in data["features"]
            ]
        except KeyError as error:
            logger.warning(f"No features found in the response: {response.text}")

            if "error" in data and data["error"].get("code") == 498:
                logger.warning("Received 498 error, retrying with new access token")
                self.access_token = get_esri_token(
                    settings.esri_auth_url,
                    settings.esri_referer,
                    settings.esri_username,
                    settings.esri_password,
                    self.client,
                )
                return self.fetch_geocodes(offset, batch_size)

            raise error


def import_geocodes(
    cursor: sqlite3.Cursor, from_datetime: datetime | None = None, is_pls: bool = False
):
    if from_datetime:
        esri_date = datetime_to_esri_datetime_utc(from_datetime)
    else:
        esri_date = None

    start_time = time.time()
    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        geocode_importer = GeocodeImporter(cursor, client, esri_date)
        if geocode_importer.requires_full_refresh:
            table_name = "lf_geocode_sp_survey_point" if is_pls else "geocode"
            logger.info(
                f"Clearing {table_name} before full geocode refresh because the live layer no longer supports incremental imports"
            )
            cursor.execute(f"DELETE FROM {table_name}")
            cursor.connection.commit()
        geocode_importer.import_geocodes(is_pls)

    logger.info(
        f"Geocodes loaded successfully ({geocode_importer.geocode_count} records) in {time.time() - start_time:.2f} seconds"
    )
