"""
Deletes LALF geocodes from SIRRTE.

Deletes geocodes in batches until no geocodes are left.
"""

import logging
import json

from address_etl.esri_rest_api import get_esri_token, get_total_count
from address_etl.settings import settings

batch_size = 2000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import httpx

    with httpx.Client(timeout=600) as client:
        access_token = get_esri_token(
            settings.esri_auth_url,
            settings.esri_referer,
            settings.esri_username,
            settings.esri_password,
            client,
        )

        while True:
            # Get geocodes count
            total_count = get_total_count(
                settings.esri_geocode_rest_api_query_url,
                client,
                access_token,
                params={
                    "where": "geocode_source = 'LALF'",
                    "f": "json",
                    "returnCountOnly": "true",
                },
            )

            logger.info(f"Total geocodes with source as LALF: {total_count}")
            if total_count == 0:
                break

            # Retrieve geocode objectids
            params = {
                "where": "geocode_source = 'LALF'",
                "outFields": "objectid",
                "returnGeometry": "false",
                "f": "json",
                "resultOffset": 0,
                "resultRecordCount": batch_size,
                "token": access_token,
            }
            response = client.get(
                settings.esri_geocode_rest_api_query_url, params=params
            )
            response.raise_for_status()
            if "error" in response.text:
                logger.error(f"Error getting geocodes: {response.text}")
                raise Exception(f"Error getting geocodes: {response.text}")

            data = response.json()
            features = data["features"]
            objectids = [feature["attributes"]["objectid"] for feature in features]

            # Delete geocodes
            params = {
                "deletes": json.dumps(objectids),
                "f": "json",
                "token": access_token,
            }
            response = client.post(
                settings.esri_geocode_rest_api_apply_edit_url, data=params
            )
            response.raise_for_status()
            if "error" in response.text:
                logger.error(f"Error deleting geocodes: {response.text}")
                raise Exception(f"Error deleting geocodes: {response.text}")

            logger.info(f"Deleted {len(objectids)} geocodes")
