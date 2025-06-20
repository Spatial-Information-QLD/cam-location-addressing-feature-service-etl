import json
import time
import logging
import sqlite3

import httpx

from address_etl.crud import delete_records_from_esri, get_esri_token
from address_etl.table_diff import compute_table_diff
from address_etl.settings import settings

logger = logging.getLogger(__name__)


def _log_diff(table_name: str, rows_deleted: list[str], rows_added: list[str]):
    if len(rows_deleted) <= 10 and len(rows_added) <= 10:
        logger.info(f"{table_name}: Deleted: {len(rows_deleted)} {rows_deleted}")
        logger.info(f"{table_name}: Added: {len(rows_added)} {rows_added}")
    else:
        logger.info(
            f"{table_name}: Deleted: {len(rows_deleted)} rows, Added: {len(rows_added)} rows"
        )


def _insert_local_auth_into_esri(la_codes: set[str], cursor: sqlite3.Cursor):
    start_time = time.time()
    batch_size = 2000
    job_id = 1

    cursor.executemany(
        "INSERT INTO local_auth_loaded (la_code) VALUES (?)",
        [(la_code,) for la_code in la_codes],
    )
    cursor.connection.commit()

    while True:
        cursor.execute(
            """
            SELECT la_code FROM local_auth_loaded
            WHERE loaded = FALSE
            LIMIT ?
            """,
            (batch_size,),
        )
        la_codes_batch = [row["la_code"] for row in cursor.fetchall()]

        if not la_codes_batch:
            break

        placeholders = ",".join("?" * len(la_codes_batch))
        cursor.execute(
            f"""
            SELECT la_code, la_name
            FROM local_auth
            WHERE la_code IN ({placeholders})
            """,
            la_codes_batch,
        )
        rows = cursor.fetchall()

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
                    }
                    for row in rows
                ]
            )
            payload = {
                "f": "json",
                "token": access_token,
                "adds": adds_data,
            }
            response = client.post(
                settings.esri_pls_local_auth_api_apply_edit_url, data=payload
            )

            if response.status_code != 200 or "error" in response.text:
                logger.error(f"Failed to insert local auth: {response.text}")
                raise Exception(f"Failed to insert local auth: {response.text}")

            placeholders = ",".join(["?"] * len(la_codes_batch))
            query = f"UPDATE local_auth_loaded SET loaded = TRUE WHERE la_code IN ({placeholders})"
            cursor.execute(query, la_codes_batch)
            cursor.connection.commit()

            logger.info(f"Inserted {len(la_codes_batch)} local auths for job {job_id}")
            job_id += 1

    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")


def _local_auth_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "la_code", "previous.local_auth", "local_auth", cursor
    )
    _log_diff("local_auth", rows_deleted, rows_added)

    if rows_deleted:
        delete_records_from_esri(
            where_clause=f"la_code IN ({','.join([str(row) for row in rows_deleted])})",
            esri_url=settings.esri_pls_local_auth_api_query_url,
            esri_apply_edits_url=settings.esri_pls_local_auth_api_apply_edit_url,
        )

    if rows_added:
        rows_union = rows_added | rows_deleted
        _insert_local_auth_into_esri(rows_union, cursor)


def _locality_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "locality_code", "previous.locality", "locality", cursor
    )
    _log_diff("locality", rows_deleted, rows_added)


def _road_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "road_id", "previous.lf_road", "lf_road", cursor
    )
    _log_diff("lf_road", rows_deleted, rows_added)


def _parcel_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "parcel_id", "previous.lf_parcel", "lf_parcel", cursor
    )
    _log_diff("lf_parcel", rows_deleted, rows_added)


def _site_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "site_id", "previous.lf_site", "lf_site", cursor
    )
    _log_diff("lf_site", rows_deleted, rows_added)


def _address_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "addr_id", "previous.lf_address", "lf_address", cursor
    )
    _log_diff("lf_address", rows_deleted, rows_added)


def compute_diff_and_sync(cursor: sqlite3.Cursor, previous_db_path: str):
    # Attach the previous ETL's database.
    cursor.execute("ATTACH DATABASE ? AS previous", (previous_db_path,))

    _local_auth_diff_and_sync(cursor)
    _locality_diff_and_sync(cursor)
    _road_diff_and_sync(cursor)
    _parcel_diff_and_sync(cursor)
    _site_diff_and_sync(cursor)
    _address_diff_and_sync(cursor)

    cursor.execute("DETACH DATABASE previous")
    cursor.connection.commit()
