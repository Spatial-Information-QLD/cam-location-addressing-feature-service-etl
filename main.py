import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pytz

from address_etl.crud import delete_records_from_esri, insert_addresses_into_esri
from address_etl.geocode import import_geocodes
from address_etl.s3 import S3, download_file, get_latest_file, upload_file
from address_etl.settings import settings
from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.table_diff import compute_table_diff
from address_etl.table_row_hash import hash_rows_in_table
from address_etl.address_tables import (
    create_table_indexes,
    create_tables,
    populate_address_current_table,
    populate_address_current_staging_table,
)
from address_etl.dynamodb_lock import get_lock, get_lock_table
from address_etl.metadata import metadata_write_end_time, metadata_write_start_time
from address_etl.time_convert import utc_to_brisbane_time

logger = logging.getLogger(__name__)

ADDRESS_PREVIOUS_DB_PATH = "/tmp/address_previous.db"
S3_FILE_PREFIX_KEY = "etl/"
LOCK_ID = "address-etl"


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    start_time = time.time()
    logger.info("Starting ETL process")

    lock_table = get_lock_table(settings.lock_table_name)
    lock = get_lock(LOCK_ID, lock_table)

    with lock.acquire():
        # Create database directory.
        Path(settings.sqlite_conn_str).parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(settings.sqlite_conn_str)
        connection.row_factory = dict_row_factory

        # Create S3 client.
        s3 = S3(settings)
        if not s3.bucket_exists(settings.s3_bucket_name):
            raise RuntimeError(f"S3 bucket {settings.s3_bucket_name} does not exist.")

        try:
            cursor = connection.cursor()
            create_tables(cursor)
            metadata_write_start_time(cursor)
            populate_address_current_staging_table(settings.sparql_endpoint, cursor)

            # Get the previous ETL's sqlite database from S3 and
            # load it into the address_previous table.
            previous_db = get_latest_file(
                settings.s3_bucket_name, s3, prefix=S3_FILE_PREFIX_KEY
            )
            previous_etl_start_time = None
            if previous_db:
                download_file(
                    settings.s3_bucket_name, previous_db, ADDRESS_PREVIOUS_DB_PATH, s3
                )

                # Attach the previous ETL's sqlite database to the connection.
                cursor.execute(
                    "ATTACH DATABASE ? AS previous", (ADDRESS_PREVIOUS_DB_PATH,)
                )

                # Get the previous ETL's start time from the metadata table.
                cursor.execute("SELECT start_time FROM previous.metadata")
                previous_etl_start_time = datetime.fromisoformat(
                    cursor.fetchone()["start_time"]
                )

                # Load the previous ETL's address_current table into the address_previous table.
                cursor.execute(
                    "INSERT INTO address_previous SELECT * FROM previous.address_current"
                )
                cursor.connection.commit()

                # Load the previous ETL's geocodes into the geocode table.
                cursor.execute("INSERT INTO geocode SELECT * FROM previous.geocode")
                cursor.connection.commit()

                cursor.execute("DETACH DATABASE previous")

            import_geocodes(cursor, previous_etl_start_time)
            create_table_indexes(cursor)
            populate_address_current_table(cursor)
            hash_rows_in_table(
                "rowid", "id", "address_current", cursor, exclude_columns=("rowid", "id")
            )

            rows_deleted, rows_added = compute_table_diff(
                "id", "address_pid", "address_previous", "address_current", cursor
            )

            if len(rows_deleted) <= 10 and len(rows_added) <= 10:
                logger.info(f"Deleted: {len(rows_deleted)} {rows_deleted}")
                logger.info(f"Added: {len(rows_added)} {rows_added}")
            else:
                logger.info(
                    f"Deleted: {len(rows_deleted)} rows, Added: {len(rows_added)} rows"
                )

            # Sync rows deleted and rows added to remote Esri service.
            # First delete the addresses.
            # Then insert the addresses.
            # The addresses that need to be inserted are the union of rows_added and rows_deleted.
            if rows_deleted:
                delete_records_from_esri(
                    where_clause=f"address_pid IN ({','.join([str(row) for row in rows_deleted])})",
                    esri_url=settings.esri_location_addressing_rest_api_query_url,
                    esri_apply_edits_url=settings.esri_location_addressing_rest_api_apply_edit_url,
                )
            if rows_union := rows_added | rows_deleted:
                insert_addresses_into_esri(
                    rows_union,
                    settings.esri_location_addressing_rest_api_apply_edit_url,
                    cursor,
                )

            current_datetime = datetime.now(pytz.UTC)
            brisbane_time = utc_to_brisbane_time(current_datetime)
            current_datetime_str = brisbane_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            metadata_write_end_time(cursor, current_datetime_str)
            upload_file(
                settings.s3_bucket_name,
                f"{S3_FILE_PREFIX_KEY}{current_datetime_str}/address.db",
                settings.sqlite_conn_str,
                s3,
            )

        finally:
            logger.info("Closing connection to SQLite database")
            connection.close()

    logger.info("ETL process completed successfully")
    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
