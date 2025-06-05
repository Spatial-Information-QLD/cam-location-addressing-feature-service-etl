"""
Loads LALF geocodes into SIRRTE.

Steps:

1. Load geocodes_for_esri.csv file into sqlite database named geocode_load.db with a column to track whether the geocodes have been loaded.
2. Spin up workers to load geocodes in batches of 10000 until all geocodes are loaded.
"""

import logging
import time
import sqlite3
import csv
from pathlib import Path
import concurrent.futures

from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.geocode_load import load_geocodes


# Constants
sqlite_conn_str = "geocode_load.db"
csv_file = "geocodes_for_esri.csv"
max_workers = 5
http_timeout_in_seconds = 600
batch_size = 10000

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def create_geocode_load_table(connection: sqlite3.Connection):
    logger.info("Creating geocode table")
    connection.execute(
        """
        CREATE TABLE geocode (
            geocode_type TEXT,
            address_pid INTEGER,
            property_name TEXT,
            building_name TEXT,
            comments TEXT,
            assoc_lotplans TEXT,
            geocode_source TEXT,
            address TEXT,
            address_status TEXT,
            longitude REAL,
            latitude REAL,
            loaded BOOLEAN DEFAULT FALSE
        )
    """
    )
    connection.commit()


def insert_geocodes_to_db(csv_file: str, connection: sqlite3.Connection):
    logger.info(f"Loading geocodes from {csv_file}")
    with open(csv_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = [row for row in reader]
        query = """
                INSERT INTO geocode (
                    geocode_type,
                    address_pid,
                    property_name,
                    building_name,
                    comments,
                    assoc_lotplans,
                    geocode_source,
                    address,
                    address_status,
                    longitude,
                    latitude
                ) VALUES (
                    :geocode_type,
                    :address_pid,
                    :property_name,
                    :building_name, 
                    :comments,
                    :assoc_lotplans,
                    :geocode_source,
                    :address,
                    :address_status,
                    :longitude,
                    :latitude
                )
            """
        logger.info(f"Loading {len(rows)} geocodes into geocode table")
        connection.executemany(query, rows)
        connection.commit()
    logger.info(f"Geocodes loaded successfully from {csv_file}")


def get_not_loaded_geocodes(connection: sqlite3.Connection):
    query = """
        SELECT rowid, * FROM geocode WHERE loaded = FALSE
    """
    return connection.execute(query).fetchall()


def main():
    Path(sqlite_conn_str).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(sqlite_conn_str)
    connection.row_factory = dict_row_factory

    errors = []

    try:
        # Only load if the table does not exist.
        result = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='geocode'"
        ).fetchone()
        if not result:
            logger.info("Table does not exist, create table and load geocodes from csv")
            create_geocode_load_table(connection)
            insert_geocodes_to_db(csv_file, connection)
        else:
            logger.info(
                "Table exists, skipping table creation and loading geocodes from csv"
            )

        geocodes = get_not_loaded_geocodes(connection)
        logger.info(f"Found {len(geocodes)} geocodes to load")

        # Create chunks of batch_size geocodes
        geocodes_chunks = [
            geocodes[i : i + batch_size] for i in range(0, len(geocodes), batch_size)
        ]
        logger.info(f"Split into {len(geocodes_chunks)} chunks for processing")

        # Spin up workers to load geocodes in batches of batch_size
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=max_workers
        ) as executor:
            futures = []
            for job_id, geocodes_chunk in enumerate(geocodes_chunks):
                futures.append(
                    executor.submit(
                        load_geocodes,
                        job_id,
                        geocodes_chunk,
                        sqlite_conn_str,
                        http_timeout_in_seconds,
                    )
                )

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error: {e}")
                    errors.append(e)

        if errors:
            logger.error(f"Errors: {errors}")
            raise Exception("Errors occurred while loading geocodes")

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        connection.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")
