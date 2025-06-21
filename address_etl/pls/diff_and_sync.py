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
        logger.info(f"{table_name}: To be deleted: {len(rows_deleted)} {rows_deleted}")
        logger.info(f"{table_name}: To be added: {len(rows_added)} {rows_added}")
    else:
        logger.info(
            f"{table_name}: To be deleted: {len(rows_deleted)} rows, To be added: {len(rows_added)} rows"
        )


def _insert_into_esri(
    table_name: str,
    pk_column_name: str,
    batched_row_select_query: str,
    esri_apply_edits_url: str,
    identifiers: set[str],
    cursor: sqlite3.Cursor,
):
    start_time = time.time()
    batch_size = 2000
    job_id = 1

    cursor.executemany(
        f"INSERT INTO {table_name}_loaded ({pk_column_name}) VALUES (?)",
        [(identifier,) for identifier in identifiers],
    )
    cursor.connection.commit()

    while True:
        cursor.execute(
            f"""
            SELECT {pk_column_name} FROM {table_name}_loaded
            WHERE loaded = FALSE
            LIMIT ?
            """,
            (batch_size,),
        )
        identifiers_batch = [row[pk_column_name] for row in cursor.fetchall()]

        if not identifiers_batch:
            break

        placeholders = ",".join("?" * len(identifiers_batch))
        cursor.execute(batched_row_select_query.format(placeholders), identifiers_batch)
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
            response = client.post(esri_apply_edits_url, data=payload)

            if response.status_code != 200 or "error" in response.text:
                logger.error(f"Failed to insert {table_name}: {response.text}")
                raise Exception(f"Failed to insert {table_name}: {response.text}")

            placeholders = ",".join(["?"] * len(identifiers_batch))
            query = f"UPDATE {table_name}_loaded SET loaded = TRUE WHERE {pk_column_name} IN ({placeholders})"
            cursor.execute(query, identifiers_batch)
            cursor.connection.commit()

            logger.info(
                f"Inserted {len(identifiers_batch)} {table_name}s for job {job_id}"
            )
            job_id += 1

    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")


def _local_auth_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "la_code", "previous.local_auth", "local_auth", cursor
    )
    _log_diff("local_auth", rows_deleted, rows_added)

    if rows_deleted:
        delete_records_from_esri(
            column_name="la_code",
            identifiers=list(rows_deleted),
            id_type=int,
            esri_url=settings.esri_pls_local_auth_api_query_url,
            esri_apply_edits_url=settings.esri_pls_local_auth_api_apply_edit_url,
        )

    if rows_added:
        rows_union = rows_added | rows_deleted
        _insert_into_esri(
            "local_auth",
            "la_code",
            "SELECT la_code, la_name FROM local_auth WHERE la_code IN ({})",
            settings.esri_pls_local_auth_api_apply_edit_url,
            rows_union,
            cursor,
        )


def _locality_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "locality_code", "previous.locality", "locality", cursor
    )
    _log_diff("locality", rows_deleted, rows_added)

    if rows_deleted:
        delete_records_from_esri(
            column_name="locality_code",
            identifiers=list(rows_deleted),
            id_type=str,
            esri_url=settings.esri_pls_locality_api_query_url,
            esri_apply_edits_url=settings.esri_pls_locality_api_apply_edit_url,
        )

    if rows_added:
        rows_union = rows_added | rows_deleted
        _insert_into_esri(
            "locality",
            "locality_code",
            "SELECT locality_code, locality_name, locality_type, la_code, state, status FROM locality WHERE locality_code IN ({})",
            settings.esri_pls_locality_api_apply_edit_url,
            rows_union,
            cursor,
        )


def _road_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "road_id", "previous.lf_road", "lf_road", cursor
    )
    _log_diff("lf_road", rows_deleted, rows_added)

    if rows_deleted:
        delete_records_from_esri(
            column_name="road_id",
            identifiers=list(rows_deleted),
            id_type=int,
            esri_url=settings.esri_pls_road_api_query_url,
            esri_apply_edits_url=settings.esri_pls_road_api_apply_edit_url,
        )

    if rows_added:
        rows_union = rows_added | rows_deleted
        _insert_into_esri(
            "lf_road",
            "road_id",
            "SELECT road_id, road_cat, road_name, road_name_suffix, road_name_type, locality_code, road_cat_desc FROM lf_road WHERE road_id IN ({})",
            settings.esri_pls_road_api_apply_edit_url,
            rows_union,
            cursor,
        )


def _parcel_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "parcel_id", "previous.lf_parcel", "lf_parcel", cursor
    )
    _log_diff("lf_parcel", rows_deleted, rows_added)

    if rows_deleted:
        delete_records_from_esri(
            column_name="parcel_id",
            identifiers=list(rows_deleted),
            id_type=int,
            esri_url=settings.esri_pls_parcel_api_query_url,
            esri_apply_edits_url=settings.esri_pls_parcel_api_apply_edit_url,
        )

    if rows_added:
        rows_union = rows_added | rows_deleted
        _insert_into_esri(
            "lf_parcel",
            "parcel_id",
            "SELECT parcel_id, plan_no, lot_no FROM lf_parcel WHERE parcel_id IN ({})",
            settings.esri_pls_parcel_api_apply_edit_url,
            rows_union,
            cursor,
        )


def _site_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "site_id", "previous.lf_site", "lf_site", cursor
    )
    _log_diff("lf_site", rows_deleted, rows_added)

    if rows_deleted:
        delete_records_from_esri(
            column_name="site_id",
            identifiers=list(rows_deleted),
            id_type=int,
            esri_url=settings.esri_pls_site_api_query_url,
            esri_apply_edits_url=settings.esri_pls_site_api_apply_edit_url,
        )

    if rows_added:
        rows_union = rows_added | rows_deleted
        _insert_into_esri(
            "lf_site",
            "site_id",
            "SELECT site_id, parent_site_id, site_type, parcel_id FROM lf_site WHERE site_id IN ({})",
            settings.esri_pls_site_api_apply_edit_url,
            rows_union,
            cursor,
        )


def _address_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "addr_id", "previous.lf_address", "lf_address", cursor
    )
    _log_diff("lf_address", rows_deleted, rows_added)

    if rows_deleted:
        delete_records_from_esri(
            column_name="addr_id",
            identifiers=list(rows_deleted),
            id_type=int,
            esri_url=settings.esri_pls_address_api_query_url,
            esri_apply_edits_url=settings.esri_pls_address_api_apply_edit_url,
        )

    if rows_added:
        rows_union = rows_added | rows_deleted
        _insert_into_esri(
            "lf_address",
            "addr_id",
            "SELECT addr_id, address_pid, parcel_id, addr_status_code, unit_type, unit_no, unit_suffix, level_type, level_no, level_suffix, street_no_first, street_no_first_suffix, street_no_last, street_no_last_suffix, road_id, site_id, location_desc, address_standard FROM lf_address WHERE addr_id IN ({})",
            settings.esri_pls_address_api_apply_edit_url,
            rows_union,
            cursor,
        )


def _geocode_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash",
        "geocode_id",
        "previous.lf_geocode_sp_survey_point",
        "lf_geocode_sp_survey_point",
        cursor,
    )
    _log_diff("lf_geocode_sp_survey_point", rows_deleted, rows_added)

    if rows_deleted:
        delete_records_from_esri(
            column_name="geocode_id",
            identifiers=list(rows_deleted),
            id_type=int,
            esri_url=settings.esri_pls_geocode_api_query_url,
            esri_apply_edits_url=settings.esri_pls_geocode_api_apply_edit_url,
        )

    if rows_added:
        rows_union = rows_added | rows_deleted
        _insert_into_esri(
            "lf_geocode_sp_survey_point",
            "geocode_id",
            "SELECT geocode_id, geocode_type, site_id, centoid_lat, centoid_lon FROM lf_geocode_sp_survey_point WHERE geocode_id IN ({})",
            settings.esri_pls_geocode_api_apply_edit_url,
            rows_union,
            cursor,
        )


def compute_diff_and_sync(cursor: sqlite3.Cursor, previous_db_path: str):
    # Attach the previous ETL's database.
    cursor.execute("ATTACH DATABASE ? AS previous", (previous_db_path,))

    _local_auth_diff_and_sync(cursor)
    _locality_diff_and_sync(cursor)
    _road_diff_and_sync(cursor)
    _parcel_diff_and_sync(cursor)
    _site_diff_and_sync(cursor)
    _address_diff_and_sync(cursor)
    _geocode_diff_and_sync(cursor)

    cursor.execute("DETACH DATABASE previous")
    cursor.connection.commit()
