import logging
import sqlite3

from address_etl.crud import delete_records_from_esri
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


def _local_auth_diff_and_sync(cursor: sqlite3.Cursor):
    rows_deleted, rows_added = compute_table_diff(
        "hash", "la_code", "previous.local_auth", "local_auth", cursor
    )
    _log_diff("local_auth", rows_deleted, rows_added)

    if rows_deleted:
        delete_records_from_esri(
            where_clause=f"la_code IN ({','.join([str(row) for row in rows_deleted])})",
            esri_url=settings.esri_location_addressing_rest_api_query_url,
            esri_apply_edits_url=settings.esri_location_addressing_rest_api_apply_edit_url,
        )


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
