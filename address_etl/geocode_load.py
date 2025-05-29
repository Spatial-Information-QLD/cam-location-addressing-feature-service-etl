import json
import logging
from typing import Any

import httpx
import backoff

from address_etl.settings import settings
from address_etl.esri_rest_api import get_esri_token

logger = logging.getLogger(__name__)


def on_backoff_handler(details):
    logger.warning(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args[0]} {args[2]} and kwargs "
        "{kwargs}".format(**details)
    )


@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPError, KeyError),
    max_time=settings.http_retry_max_time_in_seconds,
    on_backoff=on_backoff_handler,
)
def load_geocodes(job_id: int, rows: list[dict[str, Any]], http_timeout: int) -> None:
    logger.info(f"Loading geocodes for job {job_id} with {len(rows)} rows")

    with httpx.Client(timeout=http_timeout) as client:

        access_token = get_esri_token(
            esri_auth_url=settings.esri_auth_url,
            referer=settings.esri_referer,
            esri_username=settings.esri_username,
            esri_password=settings.esri_password,
            client=client,
        )
        url = settings.esri_geocode_rest_api_apply_edit_url
        payload = {
            "f": "json",
            "token": access_token,
            "adds": json.dumps(
                [
                    {
                        "attributes": row,
                        "geometry": {
                            "x": row["longitude"],
                            "y": row["latitude"],
                            "z": 0,
                            "spatialReference": {"wkid": 4283},
                        },
                    }
                    for row in rows
                ]
            ),
        }

        response = client.post(url, data=payload)

        if response.status_code != 200 or "error" in response.text:
            logger.error(f"Failed to load geocodes for job {job_id}: {response.text}")
            raise Exception(
                f"Failed to load geocodes for job {job_id}: {response.text}"
            )

        logger.info(f"Loaded geocodes for job {job_id} with {len(rows)} rows")
