import logging
import time
import sqlite3
from pathlib import Path

import boto3

from address_etl.settings import settings
from address_etl.dynamodb_lock import get_lock
from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.s3 import S3
from address_etl.pls.tables import create_tables, populate_tables

ADDRESS_PREVIOUS_DB_PATH = "/tmp/pls_previous.db"
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
            populate_tables(cursor)
        finally:
            logger.info("Closing connection to SQLite database")
            connection.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")
