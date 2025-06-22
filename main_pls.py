import logging
import time
import sqlite3
from pathlib import Path
from datetime import datetime
import pytz

import boto3

from address_etl.settings import settings
from address_etl.dynamodb_lock import get_lock
from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.s3 import S3, get_latest_file, download_file
from address_etl.pls.tables import create_tables, populate_tables
from address_etl.time_convert import utc_to_brisbane_time
from address_etl.metadata import metadata_write_start_time, metadata_write_end_time
from address_etl.s3 import upload_file
from address_etl.geocode import import_geocodes
from address_etl.table_row_hash import hash_rows_in_table
from address_etl.pls.diff_and_sync import compute_diff_and_sync

PREVIOUS_DB_PATH = "/tmp/pls_previous.db"
S3_FILE_PREFIX_KEY = "pls-etl/"
LOCK_ID = "address-etl-pls"


logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger.info("Starting ETL process")

    if settings.use_minio:
        dynamodb = boto3.resource(
            "dynamodb",
            endpoint_url="http://localhost:4566",
            region_name=settings.minio_region,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
        )
    else:
        dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(settings.lock_table_name)
    lock = get_lock(LOCK_ID, table)

    with lock.acquire():
        # Create database directory.
        Path(settings.pls_sqlite_conn_str).parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(settings.pls_sqlite_conn_str)
        connection.row_factory = dict_row_factory

        # Create S3 client.
        s3 = S3(settings)
        if not s3.bucket_exists(settings.pls_s3_bucket_name):
            raise RuntimeError(
                f"S3 bucket {settings.pls_s3_bucket_name} does not exist."
            )

        try:
            cursor = connection.cursor()
            create_tables(cursor)
            metadata_write_start_time(cursor)
            # Get the previous ETL's sqlite database from S3
            previous_db = get_latest_file(
                settings.pls_s3_bucket_name, s3, prefix=S3_FILE_PREFIX_KEY
            )
            previous_etl_start_time = None
            if previous_db:
                download_file(
                    settings.pls_s3_bucket_name, previous_db, PREVIOUS_DB_PATH, s3
                )

                # Attach the previous ETL's sqlite database to the connection.
                cursor.execute("ATTACH DATABASE ? AS previous", (PREVIOUS_DB_PATH,))

                # Get the previous ETL's start time from the metadata table.
                cursor.execute("SELECT start_time FROM metadata")
                previous_etl_start_time = datetime.fromisoformat(
                    cursor.fetchone()["start_time"]
                )

                # Load the previous ETL's geocodes into the geocode table.
                cursor.execute(
                    """
                    INSERT INTO lf_geocode_sp_survey_point
                    SELECT
                        geocode_id,
                        geocode_type,
                        address_pid,
                        NULL,
                        centoid_lat,
                        centoid_lon,
                        NULL
                    FROM previous.lf_geocode_sp_survey_point
                    """
                )
                cursor.connection.commit()

                # Load the previous ETL's mapping tables
                map_id_tables = (
                    "lf_road_id_map",
                    "lf_parcel_id_map",
                    "lf_site_id_map",
                    "lf_place_name_id_map",
                    "lf_address_id_map",
                )
                for table in map_id_tables:
                    logger.info(f"Loading {table} from previous ETL")
                    cursor.execute(
                        f"""
                        INSERT INTO {table}
                        SELECT * FROM previous.{table}
                        """
                    )
                    cursor.connection.commit()

                cursor.execute("DETACH DATABASE previous")
                cursor.connection.commit()

            import_geocodes(cursor, previous_etl_start_time, is_pls=True)
            populate_tables(cursor)

            # hash the rows in the table
            hash_rows_in_table(
                "addr_id", "hash", "lf_address", cursor, exclude_columns=("rowid",)
            )
            hash_rows_in_table(
                "place_name_id",
                "hash",
                "lf_place_name",
                cursor,
                exclude_columns=("rowid",),
            )
            hash_rows_in_table(
                "parcel_id", "hash", "lf_parcel", cursor, exclude_columns=("rowid",)
            )
            hash_rows_in_table(
                "road_id", "hash", "lf_road", cursor, exclude_columns=("rowid",)
            )
            hash_rows_in_table(
                "site_id", "hash", "lf_site", cursor, exclude_columns=("rowid",)
            )
            hash_rows_in_table(
                "la_code", "hash", "local_auth", cursor, exclude_columns=("rowid",)
            )
            hash_rows_in_table(
                "locality_code", "hash", "locality", cursor, exclude_columns=("rowid",)
            )
            hash_rows_in_table(
                "geocode_id",
                "hash",
                "lf_geocode_sp_survey_point",
                cursor,
                exclude_columns=("rowid",),
            )

            compute_diff_and_sync(cursor, PREVIOUS_DB_PATH)

            current_datetime = datetime.now(pytz.UTC)
            brisbane_time = utc_to_brisbane_time(current_datetime)
            current_datetime_str = brisbane_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            metadata_write_end_time(cursor, current_datetime_str)
            upload_file(
                settings.pls_s3_bucket_name,
                f"{S3_FILE_PREFIX_KEY}{current_datetime_str}/pls.db",
                settings.pls_sqlite_conn_str,
                s3,
            )
        finally:
            logger.info("Closing connection to SQLite database")
            connection.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")
