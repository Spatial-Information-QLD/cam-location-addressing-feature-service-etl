import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import backoff
import httpx

from address_etl.esri_rest_api import get_count, get_esri_token
from address_etl.geocode import get_layer_url, on_backoff_handler
from address_etl.settings import settings
from address_etl.time_convert import datetime_to_esri_datetime_utc

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AddressIriPidLayerSchema:
    object_id_field: str
    address_iri_field: str
    address_pid_field: str
    last_edited_field: str | None

    @property
    def supports_incremental_import(self) -> bool:
        return self.last_edited_field is not None


def get_address_iri_pid_layer_schema(
    layer_definition: dict[str, Any],
) -> AddressIriPidLayerSchema:
    field_names = {field["name"] for field in layer_definition["fields"]}
    object_id_field = (
        layer_definition.get("objectIdField")
        or layer_definition.get("objectIdFieldName")
        or "objectid"
    )

    if "iri" in field_names:
        address_iri_field = "iri"
    elif "address_iri" in field_names:
        address_iri_field = "address_iri"
    else:
        raise RuntimeError("Address IRI to PID layer schema is missing address IRI field")

    if "pid" in field_names:
        address_pid_field = "pid"
    elif "address_pid" in field_names:
        address_pid_field = "address_pid"
    else:
        raise RuntimeError("Address IRI to PID layer schema is missing address PID field")

    return AddressIriPidLayerSchema(
        object_id_field=object_id_field,
        address_iri_field=address_iri_field,
        address_pid_field=address_pid_field,
        last_edited_field="last_edited_date" if "last_edited_date" in field_names else None,
    )


def build_address_iri_pid_where_clause(
    schema: AddressIriPidLayerSchema,
    esri_date: str | None,
) -> str:
    if esri_date and schema.last_edited_field:
        return f"{schema.last_edited_field} >= DATE '{esri_date}'"

    if esri_date and not schema.last_edited_field:
        logger.warning(
            "Address IRI to PID layer no longer exposes last_edited_date; falling back to a full refresh"
        )

    return "1=1"


def normalize_address_iri_pid_feature(
    feature: dict[str, Any],
    schema: AddressIriPidLayerSchema,
) -> dict[str, str]:
    attrs = feature["attributes"]

    return {
        "objectid": str(attrs[schema.object_id_field]),
        "address_iri": str(attrs[schema.address_iri_field]),
        "address_pid": str(attrs[schema.address_pid_field]),
    }


def load_address_pid_mappings(
    cursor: sqlite3.Cursor, address_iris: list[str] | None = None
) -> dict[str, str]:
    if address_iris:
        placeholders = ",".join("?" for _ in address_iris)
        cursor.execute(
            f"""
            SELECT address_iri, address_pid
            FROM address_iri_pid_map
            WHERE address_iri IN ({placeholders})
            """,
            address_iris,
        )
    else:
        cursor.execute(
            """
            SELECT address_iri, address_pid
            FROM address_iri_pid_map
            """
        )

    return {
        row["address_iri"]: row["address_pid"]
        for row in cursor.fetchall()
    }


def save_address_pid_mappings(
    cursor: sqlite3.Cursor,
    mappings: list[dict[str, str]],
) -> None:
    if not mappings:
        return

    cursor.executemany(
        """
        INSERT INTO address_iri_pid_map (address_iri, address_pid)
        VALUES (?, ?)
        ON CONFLICT(address_iri)
        DO UPDATE SET address_pid = excluded.address_pid
        """,
        [(mapping["address_iri"], mapping["address_pid"]) for mapping in mappings],
    )
    cursor.connection.commit()


class AddressIriPidImporter:
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
        self.where_clause = build_address_iri_pid_where_clause(self.schema, esri_date)
        self.requires_full_refresh = bool(
            esri_date and not self.schema.supports_incremental_import
        )
        self.mapping_count = get_count(
            self.where_clause,
            settings.esri_address_iri_pid_map_query_url,
            self.client,
            self.access_token,
        )

    def fetch_layer_schema(self) -> AddressIriPidLayerSchema:
        params = {"f": "json", "token": self.access_token}
        response = self.client.get(
            get_layer_url(settings.esri_address_iri_pid_map_query_url),
            params=params,
        )
        response.raise_for_status()
        payload = response.json()

        if "error" in payload:
            error = payload["error"]
            raise RuntimeError(
                "Error fetching address IRI to PID layer schema: "
                f"ESRI error {error.get('code')}: {error.get('message')}"
            )

        if "fields" not in payload:
            raise RuntimeError(
                "Error fetching address IRI to PID layer schema: "
                f"expected 'fields' in response, got: {response.text[:1000]}"
            )

        return get_address_iri_pid_layer_schema(payload)

    def import_mappings(self) -> None:
        logger.info(f"Fetching {self.mapping_count} address IRI to PID mappings")
        batch_size = 2000
        for offset in range(0, self.mapping_count, batch_size):
            mappings = self.fetch_mappings(offset, batch_size)
            if not mappings:
                logger.warning(f"No address IRI to PID mappings found for offset {offset}")
                continue

            save_address_pid_mappings(self.cursor, mappings)

    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPError, KeyError),
        max_time=settings.http_retry_max_time_in_seconds,
        on_backoff=on_backoff_handler,
    )
    def fetch_mappings(self, offset: int, batch_size: int) -> list[dict[str, str]]:
        params = {
            "where": self.where_clause,
            "outFields": ",".join(
                (
                    self.schema.object_id_field,
                    self.schema.address_iri_field,
                    self.schema.address_pid_field,
                )
            ),
            "returnGeometry": "false",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "token": self.access_token,
            "f": "json",
        }
        response = self.client.get(
            settings.esri_address_iri_pid_map_query_url,
            params=params,
        )
        response.raise_for_status()
        data = response.json()

        try:
            return [
                normalize_address_iri_pid_feature(feature, self.schema)
                for feature in data["features"]
            ]
        except KeyError as error:
            logger.warning(
                "No address IRI to PID features found in the response: %s",
                response.text,
            )

            if "error" in data and data["error"].get("code") == 498:
                logger.warning("Received 498 error, retrying with new access token")
                self.access_token = get_esri_token(
                    settings.esri_auth_url,
                    settings.esri_referer,
                    settings.esri_username,
                    settings.esri_password,
                    self.client,
                )
                return self.fetch_mappings(offset, batch_size)

            raise error


def import_address_pid_mappings(
    cursor: sqlite3.Cursor,
    from_datetime: datetime | None = None,
) -> None:
    esri_date = datetime_to_esri_datetime_utc(from_datetime) if from_datetime else None

    start_time = time.time()
    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        importer = AddressIriPidImporter(cursor, client, esri_date)
        if importer.requires_full_refresh:
            logger.info(
                "Clearing address_iri_pid_map before full refresh because the live layer "
                "no longer supports incremental imports"
            )
            cursor.execute("DELETE FROM address_iri_pid_map")
            cursor.connection.commit()

        importer.import_mappings()

    logger.info(
        "Address IRI to PID mappings loaded successfully (%s records) in %.2f seconds",
        importer.mapping_count,
        time.time() - start_time,
    )
