"""
Deletes addresses from SIRRTE
"""

import logging
import json
import time

import httpx

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
    start_time = time.time()

    with httpx.Client(timeout=600) as client:

        token_use = 0

        while True:

            if token_use == 0:
                logger.info("Getting ESRI token")
                access_token = get_esri_token(
                    settings.esri_auth_url,
                    settings.esri_referer,
                    settings.esri_username,
                    settings.esri_password,
                    client,
                )
                logger.info("ESRI token obtained")
                token_use = 10

            # # Get addresses
            # total_count = get_total_count(
            #     settings.esri_location_addressing_rest_api_query_url,
            #     client,
            #     access_token,
            #     params={
            #         "where": "1=1",
            #         "f": "json",
            #         "returnCountOnly": "true",
            #     },
            # )

            # logger.info(f"Total addresses: {total_count}")
            # if total_count == 0:
            #     break

            # Retrieve addresses
            params = {
                "where": "1=1",
                "returnIdsOnly": "true",
                "returnGeometry": "false",
                "f": "json",
                "resultOffset": 0,
                "resultRecordCount": batch_size,
                "token": access_token,
            }
            response = client.get(
                settings.esri_location_addressing_rest_api_query_url, params=params
            )

            try:
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Error getting addresses: {response.text}")
                raise e

            if "error" in response.text:
                logger.error(f"Error getting addresses: {response.text}")
                raise Exception(f"Error getting addresses: {response.text}")

            data = response.json()
            objectids = data["objectIds"]

            if not objectids or len(objectids) == 0:
                break

            # Delete addresses
            params = {
                "deletes": json.dumps(objectids),
                "f": "json",
                "token": access_token,
            }
            response = client.post(
                settings.esri_location_addressing_rest_api_apply_edit_url, data=params
            )

            try:
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Error deleting addresses: {response.text}")
                raise e

            if "error" in response.text:
                logger.error(f"Error deleting addresses: {response.text}")
                raise Exception(f"Error deleting addresses: {response.text}")

            logger.info(f"Deleted {len(objectids)} addresses")

            token_use -= 1

    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")
