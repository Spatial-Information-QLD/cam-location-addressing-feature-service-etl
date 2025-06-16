import time
import logging
import sqlite3
from datetime import datetime
from typing import Any

import httpx
import backoff
from rich.progress import track

from address_etl.esri_rest_api import get_esri_token, get_count
from address_etl.settings import settings
from address_etl.time_convert import datetime_to_esri_datetime_utc

logger = logging.getLogger(__name__)


def on_backoff_handler(details):
    """Handler for backoff errors"""
    logger.warning(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


def insert_geocodes(cursor: sqlite3.Cursor, features: list[dict[str, Any]]):
    """Insert geocodes into the database"""
    for feature in features:
        attrs = feature["attributes"]
        geom = feature["geometry"]

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

        self.where_clause = "(geocode_status IS NULL OR geocode_status <> 'H') AND LOWER(geocode_source) NOT LIKE 'derived from geoscape buildings%' AND LOWER(geocode_source) NOT LIKE 'asa geocodes%'"
        if esri_date:
            self.where_clause += f" AND last_edited_date >= DATE '{esri_date}'"

        self.geocode_count = get_count(
            self.where_clause,
            settings.esri_geocode_rest_api_query_url,
            self.client,
            self.access_token,
        )

    def import_geocodes(self) -> None:
        logger.info(f"Fetching {self.geocode_count} geocodes")
        batch_size = 2000
        for offset in track(
            range(0, self.geocode_count, batch_size),
            description="Processing geocodes",
        ):
            features = self.fetch_geocodes(offset, batch_size)
            if not features:
                logger.warning(f"No geocodes found for offset {offset}")
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
            "outFields": "objectid,geocode_type,address_pid",
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "token": self.access_token,
            "f": "json",
        }
        response = self.client.get(
            settings.esri_geocode_rest_api_query_url, params=params
        )
        response.raise_for_status()
        data = response.json()

        try:
            return data["features"]
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


def import_geocodes(cursor: sqlite3.Cursor, from_datetime: datetime | None = None):
    if from_datetime:
        esri_date = datetime_to_esri_datetime_utc(from_datetime)
    else:
        esri_date = None

    start_time = time.time()
    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        geocode_importer = GeocodeImporter(cursor, client, esri_date)
        geocode_importer.import_geocodes()

    logger.info(
        f"Geocodes loaded successfully ({geocode_importer.geocode_count} records) in {time.time() - start_time:.2f} seconds"
    )
