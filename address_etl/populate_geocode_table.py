import logging
import sqlite3
import time
from typing import Any

import backoff
import httpx

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


# In some cases, the service returns a 200 response but no features are returned.
@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPError, KeyError),
    max_time=settings.http_retry_max_time_in_minutes,
    on_backoff=on_backoff_handler,
)
def fetch_geocodes(
    client: httpx.Client, access_token: str, offset: int, batch_size: int = 10000
) -> list[dict[str, Any]]:
    """Fetch a batch of geocodes from the service"""
    params = {
        "where": "1=1",
        "outFields": "geocode_type,address_pid",
        "returnGeometry": "true",
        "resultOffset": offset,
        "resultRecordCount": batch_size,
        "f": "json",
        "token": access_token,
    }

    response = client.get(settings.esri_geocode_rest_api_url, params=params)
    response.raise_for_status()
    return response.json()["features"]


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


def populate_geocode_table(cursor: sqlite3.Cursor):
    """
    Scrape the geocodes from the Esri feature service and cache them in the database.
    """
    start_time = time.time()
    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        access_token = get_esri_token(
            settings.esri_auth_url,
            settings.esri_referer,
            settings.esri_username,
            settings.esri_password,
            client,
        )
        total_count = get_total_count(
            settings.esri_geocode_rest_api_url, client, access_token
        )
        logger.info(f"Total records to process: {total_count}")

        # Process in batches
        batch_size = 10_000
        for offset in range(0, total_count, batch_size):
            logger.info(
                f"Processing records {offset} to {min(offset + batch_size, total_count)}"
            )

            features = fetch_geocodes(client, access_token, offset, batch_size)
            logger.info(f"Fetched {len(features)} features")
            insert_geocodes(cursor, features)
            cursor.connection.commit()

        logger.info(
            f"Geocodes loaded successfully ({total_count} records) in {time.time() - start_time:.2f} seconds"
        )
