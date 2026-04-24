import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import boto3
import pytz

from address_etl.address_iri_pid_map import import_address_pid_mappings
from address_etl.dynamodb_lock import get_lock
from address_etl.geocode import import_geocodes
from address_etl.kafka import publish_presigned_url
from address_etl.metadata import metadata_write_end_time, metadata_write_start_time
from address_etl.pls.tables import (
    create_tables,
    populate_tables,
    prune_geocodes_without_addresses,
)
from address_etl.s3 import S3, download_file, get_latest_file, upload_file
from address_etl.settings import settings
from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.time_convert import utc_to_brisbane_time

PREVIOUS_DB_PATH = "/tmp/pls_previous.db"
S3_FILE_PREFIX_KEY = "pls-etl/"
LOCK_ID = "address-etl-pls"


logger = logging.getLogger(__name__)


def format_kafka_timestamp(dt: datetime) -> str:
    return dt.astimezone(pytz.UTC).isoformat()


def build_artifact_headers(
    *,
    etl_started_at: datetime,
    etl_finished_at: datetime,
    artifact_uploaded_at: datetime,
    duration_seconds: float,
    s3_bucket: str,
    s3_key: str,
    presigned_url_expiry_seconds: int,
) -> dict[str, str]:
    return {
        "etl-name": "pls",
        "etl-started-at": format_kafka_timestamp(etl_started_at),
        "etl-finished-at": format_kafka_timestamp(etl_finished_at),
        "artifact-uploaded-at": format_kafka_timestamp(artifact_uploaded_at),
        "etl-duration-seconds": f"{duration_seconds:.3f}",
        "s3-bucket": s3_bucket,
        "s3-key": s3_key,
        "presigned-url-expiry-seconds": str(presigned_url_expiry_seconds),
    }


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
        etl_started_at = datetime.now(pytz.UTC)
        etl_started_at_brisbane = utc_to_brisbane_time(etl_started_at)
        etl_started_at_str = etl_started_at_brisbane.strftime("%Y-%m-%dT%H:%M:%S%z")

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
            metadata_write_start_time(cursor, etl_started_at_str)
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
                cursor.execute("SELECT start_time FROM previous.metadata")
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

                cursor.execute(
                    """
                    SELECT name FROM previous.sqlite_master
                    WHERE type = 'table' AND name = 'geocode_type_code'
                    """
                )
                if cursor.fetchone():
                    cursor.execute(
                        """
                        INSERT INTO geocode_type_code
                        SELECT * FROM previous.geocode_type_code
                        """
                    )
                    cursor.connection.commit()

                cursor.execute(
                    """
                    SELECT name FROM previous.sqlite_master
                    WHERE type = 'table' AND name = 'address_iri_pid_map'
                    """
                )
                if cursor.fetchone():
                    cursor.execute(
                        """
                        INSERT INTO address_iri_pid_map
                        SELECT * FROM previous.address_iri_pid_map
                        """
                    )
                    cursor.connection.commit()

                cursor.execute("DETACH DATABASE previous")
                cursor.connection.commit()

            import_address_pid_mappings(cursor, previous_etl_start_time)
            import_geocodes(cursor, previous_etl_start_time)
            populate_tables(cursor)
            prune_geocodes_without_addresses(cursor)

            etl_finished_at = datetime.now(pytz.UTC)
            etl_finished_at_brisbane = utc_to_brisbane_time(etl_finished_at)
            etl_finished_at_str = etl_finished_at_brisbane.strftime("%Y-%m-%dT%H:%M:%S%z")
            metadata_write_end_time(cursor, etl_finished_at_str)

            s3_key = f"{S3_FILE_PREFIX_KEY}{etl_finished_at_str}/pls.db"
            presigned_url = upload_file(
                settings.pls_s3_bucket_name,
                s3_key,
                settings.pls_sqlite_conn_str,
                s3,
                presigned_url_expiry_seconds=settings.s3_presigned_url_expiry_seconds,
            )
            artifact_uploaded_at = datetime.now(pytz.UTC)
            publish_presigned_url(
                presigned_url,
                build_artifact_headers(
                    etl_started_at=etl_started_at,
                    etl_finished_at=etl_finished_at,
                    artifact_uploaded_at=artifact_uploaded_at,
                    duration_seconds=(etl_finished_at - etl_started_at).total_seconds(),
                    s3_bucket=settings.pls_s3_bucket_name,
                    s3_key=s3_key,
                    presigned_url_expiry_seconds=settings.s3_presigned_url_expiry_seconds,
                ),
            )
        finally:
            logger.info("Closing connection to SQLite database")
            connection.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")
