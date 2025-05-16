import logging
import sqlite3
import time

import httpx
from rich.progress import track

from address_etl.address_concat import compute_address_concatenation
from address_etl.address_current_staging_table import (
    populate_address_current_staging_rows,
)
from address_etl.address_iris import get_address_iris
from address_etl.address_rows import get_address_rows
from address_etl.geocode_table import populate_geocode_table
from address_etl.settings import settings
from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.table_diff import compute_table_diff
from address_etl.table_row_hash import hash_rows_in_table
from address_etl.tables import create_table_indexes, create_tables

logger = logging.getLogger(__name__)


def populate_address_current_staging_table(
    sparql_endpoint: str, cursor: sqlite3.Cursor
):
    """
    Retrieve all address IRIs from the SPARQL endpoint, page through and retrieve the address data in chunks of 1000, and insert them into the address_current_staging table.
    """
    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        logger.info("Retrieving address IRIs from SPARQL endpoint")
        address_iris = get_address_iris(
            sparql_endpoint, client, settings.address_iri_limit
        )
        logger.info(f"Retrieved {len(address_iris)} address IRIs to process")

        # Create chunks of 1000 IRIs
        address_iri_chunks = [
            address_iris[i : i + 1000] for i in range(0, len(address_iris), 1000)
        ]
        logger.info(f"Split into {len(address_iri_chunks)} chunks for processing")

        for address_iri_chunk in track(
            address_iri_chunks, description="Processing address IRIs"
        ):
            rows = get_address_rows(address_iri_chunk, sparql_endpoint, client)
            modified_rows = []
            for row in rows:
                data = {}
                data["id"] = None
                data["lot"] = row.get("lot", {}).get("value")
                data["plan"] = row.get("plan", {}).get("value")
                data["unit_type"] = row.get("unit_type", {}).get("value")
                data["unit_number"] = row.get("unit_number", {}).get("value")
                data["unit_suffix"] = row.get("unit_suffix", {}).get("value")
                data["floor_type"] = row.get("floor_type", {}).get("value")
                data["floor_number"] = row.get("floor_number", {}).get("value")
                data["floor_suffix"] = row.get("floor_suffix", {}).get("value")
                data["property_name"] = row.get("property_name", {}).get("value")
                data["street_no_1"] = row.get("street_no_1", {}).get("value")
                data["street_no_1_suffix"] = row.get("street_no_1_suffix", {}).get(
                    "value"
                )
                data["street_no_2"] = row.get("street_no_2", {}).get("value")
                data["street_no_2_suffix"] = row.get("street_no_2_suffix", {}).get(
                    "value"
                )
                data["street_number"] = row.get("street_number", {}).get("value")
                data["street_name"] = row.get("street_name", {}).get("value")
                data["street_type"] = row.get("street_type", {}).get("value")
                data["street_suffix"] = row.get("street_suffix", {}).get("value")
                data["street_full"] = row.get("street_full", {}).get("value")
                data["locality"] = row.get("locality", {}).get("value")
                data["local_authority"] = row.get("local_authority", {}).get("value")
                data["state"] = row.get("state", {}).get("value")
                data["address"] = compute_address_concatenation(row)
                data["address_status"] = row.get("address_status", {}).get("value")
                data["address_standard"] = row.get("address_standard", {}).get("value")
                data["lotplan_status"] = row.get("lotplan_status", {}).get("value")
                data["address_pid"] = row.get("address_pid", {}).get("value")
                # Geocodes are populated later.
                data["geocode_type"] = None
                data["latitude"] = None
                data["longitude"] = None
                modified_rows.append(data)

            populate_address_current_staging_rows(modified_rows, cursor)


def populate_address_current_table(cursor: sqlite3.Cursor):
    """
    Populate the address_current table with data from the address_current_staging table joined
    with the geocode table on the address_pid column.

    Note that since we're performing a join, if the address_pid does not exist in the geocode table,
    the row will not be inserted into the address_current table.
    """
    cursor.execute(
        """
        INSERT INTO address_current
        SELECT
            a.id,
            a.lot,
            a.plan,
            a.unit_type,
            a.unit_number,
            a.unit_suffix,
            a.floor_type,
            a.floor_number,
            a.floor_suffix,
            a.property_name,
            a.street_no_1,
            a.street_no_1_suffix,
            a.street_no_2,
            a.street_no_2_suffix,
            a.street_number,
            a.street_name,
            a.street_type,
            a.street_suffix,
            a.street_full,
            a.locality,
            a.local_authority,
            a.state,
            a.address,
            a.address_status,
            a.address_standard,
            a.lotplan_status,
            a.address_pid,
            g.geocode_type,
            g.latitude,
            g.longitude
        FROM address_current_staging a
        JOIN geocode g ON a.address_pid = g.address_pid
    """
    )
    cursor.connection.commit()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    start_time = time.time()
    logger.info("Starting ETL process")

    connection = sqlite3.connect(settings.sqlite_conn_str)
    connection.row_factory = dict_row_factory
    try:
        cursor = connection.cursor()
        create_tables(cursor)
        populate_address_current_staging_table(settings.sparql_endpoint, cursor)
        if settings.populate_geocode_table:
            populate_geocode_table(cursor)
        create_table_indexes(cursor)
        populate_address_current_table(cursor)
        hash_rows_in_table("address_current", cursor)
        rows_deleted, rows_added = compute_table_diff(
            "address_previous", "address_current", cursor
        )
        print(f"Deleted: {len(rows_deleted)}")
        print(f"Added: {len(rows_added)}")

        # TODO: create indexes on all tables
        #       index address_pid and id columns
    finally:
        logger.info("Closing connection to SQLite database")
        connection.close()

    logger.info("ETL process completed successfully")
    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
