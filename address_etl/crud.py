import json
import time
import logging
import sqlite3
from typing import Sequence

import httpx

from address_etl.esri_rest_api import get_esri_token
from address_etl.settings import settings

logger = logging.getLogger(__name__)


def delete_records_from_esri(
    column_name: str,
    identifiers: list,
    id_type: type,
    esri_url: str,
    esri_apply_edits_url: str,
):
    start_time = time.time()
    batch_size = 2000

    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        token_use = 0
        total_deleted = 0

        # Process identifiers in batches
        for i in range(0, len(identifiers), batch_size):
            batch_identifiers = identifiers[i : i + batch_size]

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

            # Build where clause for this batch
            if id_type == str:
                placeholders = ",".join(
                    "'" + identifier + "'" for identifier in batch_identifiers
                )
            else:
                placeholders = ",".join(
                    str(identifier) for identifier in batch_identifiers
                )
            where_clause = f"{column_name} IN ({placeholders})"

            # Get objectids of records to delete for this batch
            params = {
                "where": where_clause,
                "outFields": "objectid",
                "returnGeometry": "false",
                "f": "json",
                "resultOffset": 0,
                "resultRecordCount": batch_size,
                "token": access_token,
            }
            response = client.post(esri_url, data=params)

            try:
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Error getting records: {response.text}")
                raise e

            if "error" in response.text:
                logger.error(f"Error getting records: {response.text}")
                raise Exception(f"Error getting records: {response.text}")

            data = response.json()
            features = data["features"]

            if not features:
                logger.info(f"No records found for batch {i//batch_size + 1}")
                continue

            objectids = [feature["attributes"]["objectid"] for feature in features]

            # Delete records
            params = {
                "deletes": json.dumps(objectids),
                "f": "json",
                "token": access_token,
            }
            response = client.post(esri_apply_edits_url, data=params)

            try:
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Error deleting records: {response.text}")
                raise e

            if "error" in response.text:
                logger.error(f"Error deleting records: {response.text}")
                raise Exception(f"Error deleting records: {response.text}")

            total_deleted += len(objectids)
            logger.info(
                f"Deleted {len(objectids)} records from batch {i//batch_size + 1}"
            )

            token_use -= 1

    logger.info(f"Total records deleted: {total_deleted}")
    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")


def insert_addresses_into_esri(
    address_pids: Sequence[str], esri_url: str, cursor: sqlite3.Cursor
):
    start_time = time.time()
    batch_size = 2000
    job_id = 1

    # Insert address_pids into address_current_loaded table
    # This table tracks which address_pids have been inserted into ESRI.
    cursor.executemany(
        "INSERT INTO address_current_loaded (address_pid) VALUES (?)",
        [(pid,) for pid in address_pids],
    )
    cursor.connection.commit()

    while True:
        # Pop first 2000 address_pids
        cursor.execute(
            """
            SELECT address_pid FROM address_current_loaded
            WHERE loaded = FALSE
            LIMIT ?
            """,
            (batch_size,),
        )
        address_pids_batch = [row["address_pid"] for row in cursor.fetchall()]

        if not address_pids_batch:
            break

        # Grab the addresses from the address_current table
        placeholders = ",".join("?" * len(address_pids_batch))
        cursor.execute(
            f"""
            SELECT
                lot,
                plan,
                address,
                unit_number,
                unit_type,
                street_number,
                street_name,
                street_type,
                state,
                street_suffix,
                property_name,
                street_no_1,
                street_no_1_suffix,
                street_no_2,
                street_no_2_suffix,
                street_full,
                locality,
                local_authority,
                address_status,
                address_standard,
                lotplan_status,
                address_pid,
                geocode_type,
                latitude,
                longitude
            FROM address_current
            WHERE address_pid IN ({placeholders})
            """,
            address_pids_batch,
        )
        addresses = cursor.fetchall()

        # Insert address_pids_batch into esri
        with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
            access_token = get_esri_token(
                settings.esri_auth_url,
                settings.esri_referer,
                settings.esri_username,
                settings.esri_password,
                client,
            )
            adds_data = json.dumps(
                [
                    {
                        "attributes": row,
                        "geometry": {
                            "x": row["longitude"],
                            "y": row["latitude"],
                            "spatialReference": {"wkid": 4283},
                        },
                    }
                    for row in addresses
                ]
            )
            payload = {
                "f": "json",
                "token": access_token,
                "adds": adds_data,
            }
            response = client.post(esri_url, data=payload)

            if response.status_code != 200 or "error" in response.text:
                logger.error(f"Failed to insert addresses: {response.text}")
                raise Exception(f"Failed to insert addresses: {response.text}")

            placeholders = ", ".join(["?"] * len(address_pids_batch))
            query = f"UPDATE address_current_loaded SET loaded = TRUE WHERE address_pid IN ({placeholders})"
            cursor.execute(query, address_pids_batch)
            cursor.connection.commit()

            logger.info(
                f"Inserted {len(address_pids_batch)} addresses for job {job_id}"
            )
            job_id += 1

    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")
