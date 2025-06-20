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


def get_geocode_count(
    esri_url: str, client: httpx.Client, access_token: str, params: dict | None = None
) -> int:
    """Get the total number of geocodes from the service"""
    params = params or {
        "where": "LOWER(geocode_source) NOT LIKE 'derived from geoscape buildings%' AND LOWER(geocode_source) NOT LIKE 'asa geocodes%'",
        "returnCountOnly": "true",
        "f": "json",
    }
    params["token"] = access_token

    response = client.get(esri_url, params=params)
    try:
        response.raise_for_status()
        return int(response.json()["count"])
    except Exception as e:
        logger.error(f"Error getting total count: {response.text}")
        raise e


class GeocodeTablePopulator:
    """
    Populate the geocode table with data from the Esri feature service.

    This class handles refreshing the access token if it expires.
    """

    def __init__(
        self,
        cursor: sqlite3.Cursor,
        client: httpx.Client,
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

        self.total_count = get_geocode_count(
            settings.esri_geocode_rest_api_query_url, client, self.access_token
        )

    def populate(self):
        logger.info(f"Total records to process: {self.total_count}")
        batch_size = 2000
        for offset in track(
            range(0, self.total_count, batch_size),
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
            "where": "LOWER(geocode_source) NOT LIKE 'derived from geoscape buildings%' AND LOWER(geocode_source) NOT LIKE 'asa geocodes%'",
            "outFields": "geocode_type,address_pid",
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "f": "json",
            "token": self.access_token,
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


def fetch_geocodes_in_debug_mode(cursor: sqlite3.Cursor, client: httpx.Client):
    """
    Fetch geocodes in debug mode.

    Only fetch the geocodes that we have address_pid values for.
    """
    access_token = get_esri_token(
        settings.esri_auth_url,
        settings.esri_referer,
        settings.esri_username,
        settings.esri_password,
        client,
    )

    logger.info("Fetching geocodes in debug mode")

    # Get the address_pid values from the address_current_staging table
    cursor.execute("SELECT address_pid FROM address_current_staging")
    address_pids = [str(row["address_pid"]) for row in cursor.fetchall()]

    logger.info(f"Found {len(address_pids)} address_pid values")

    # Fetch the geocodes for the address_pid values
    params = {
        "where": f"geocode_source = 'LALF' AND address_pid IN ({', '.join(address_pids)})",
        "outFields": "geocode_type,address_pid",
        "returnGeometry": "true",
        "f": "json",
        "token": access_token,
    }

    response = client.get(settings.esri_geocode_rest_api_query_url, params=params)
    response.raise_for_status()

    try:
        data = response.json()
        features = data["features"]
        logger.info(f"Found {len(features)} geocodes")
        insert_geocodes(cursor, features)
    except KeyError:
        logger.error(f"No features found in the response: {response.text}")
        raise

    logger.info("Geocodes loaded successfully")


def populate_geocode_table(cursor: sqlite3.Cursor):
    """
    Scrape the geocodes from the Esri feature service and cache them in the database.
    """
    start_time = time.time()
    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        if settings.geocode_debug:
            fetch_geocodes_in_debug_mode(cursor, client)
        else:
            geocode_populator = GeocodeTablePopulator(cursor, client)
            geocode_populator.populate()
            logger.info(
                f"Geocodes loaded successfully ({geocode_populator.total_count} records) in {time.time() - start_time:.2f} seconds"
            )
