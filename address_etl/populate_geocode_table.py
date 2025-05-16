import logging
import sqlite3
import time
from typing import Any

import backoff
import httpx
from rich.progress import track

from address_etl.esri_rest_api import get_esri_token
from address_etl.settings import settings

logger = logging.getLogger(__name__)


def get_total_count(esri_url: str, client: httpx.Client, access_token: str) -> int:
    """Get the total number of records from the service"""
    params = {
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
        "token": access_token,
    }

    response = client.get(esri_url, params=params)
    response.raise_for_status()
    return response.json()["count"]


def on_backoff_handler(details):
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
            INSERT INTO geocode (address_pid, geocode_type, longitude, latitude)
            VALUES (?, ?, ?, ?)
        """,
            (attrs["address_pid"], attrs["geocode_type"], geom["x"], geom["y"]),
        )


class GeocodeTablePopulator:
    """
    Populate the geocode table with data from the Esri feature service.

    This class handles refreshing the access token if it expires.
    """

    def __init__(
        self,
        cursor: sqlite3.Cursor,
        client: httpx.Client,
        total_count: int | None = None,
    ):
        self.cursor = cursor
        self.client = client
        self.access_token = get_esri_token(
            settings.esri_auth_url,
            settings.esri_referer,
            settings.esri_username,
            settings.esri_password,
            client,
        )

        self.total_count = total_count or get_total_count(
            settings.esri_geocode_rest_api_url, client, self.access_token
        )

    def populate(self):
        logger.info(f"Total records to process: {self.total_count}")
        batch_size = 10_000
        for offset in track(
            range(0, self.total_count, batch_size),
            description="Processing geocodes",
        ):
            features = self.fetch_geocodes(offset, batch_size)
            insert_geocodes(self.cursor, features)
            self.cursor.connection.commit()

    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPError, KeyError),
        max_time=settings.http_retry_max_time_in_seconds,
        on_backoff=on_backoff_handler,
    )
    def fetch_geocodes(
        self, offset: int, batch_size: int = 10000
    ) -> list[dict[str, Any]]:
        """Fetch a batch of geocodes from the service"""
        params = {
            "where": "1=1",
            "outFields": "geocode_type,address_pid",
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "f": "json",
            "token": self.access_token,
        }

        response = self.client.get(settings.esri_geocode_rest_api_url, params=params)
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


def populate_geocode_table(cursor: sqlite3.Cursor):
    """
    Scrape the geocodes from the Esri feature service and cache them in the database.
    """
    start_time = time.time()
    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        geocode_populator = GeocodeTablePopulator(
            cursor, client, settings.geocode_limit
        )
        geocode_populator.populate()

        logger.info(
            f"Geocodes loaded successfully ({geocode_populator.total_count} records) in {time.time() - start_time:.2f} seconds"
        )
