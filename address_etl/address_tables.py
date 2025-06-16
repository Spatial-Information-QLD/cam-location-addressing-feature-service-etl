import logging
import sqlite3

import httpx
from rich.progress import track

from address_etl.address_concat import compute_address_concatenation
from address_etl.address_iris import get_address_iris
from address_etl.address_rows import get_address_rows
from address_etl.settings import settings
from address_etl.tables import create_metadata_table

logger = logging.getLogger(__name__)


def create_tables(cursor: sqlite3.Cursor):
    """
    Create the tables required for the address ETL process

    The tables are:

    - geocode - the table to store the geocode data pulled from SIRRTE.
    - address_previous - the table to store the location address table from the previous ETL run.
    - address_current_staging - the table to store the generated location address data from the current ETL run.
    - address_current - the final location address table. This table contains the row hash to perform the diffing algorithm with the previous ETL run.
    - metadata - the table to store the metadata for the ETL run.
    """

    logger.info("Creating geocode table")
    cursor.execute(
        """
        CREATE TABLE geocode (
            geocode_id TEXT PRIMARY KEY,
            geocode_type TEXT,
            address_pid TEXT NOT NULL,
            longitude REAL,
            latitude REAL
        )
    """
    )

    logger.info("Creating address_previous table")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS address_previous (
            id TEXT,
            lot TEXT,
            plan TEXT,
            unit_type TEXT,
            unit_number TEXT,
            unit_suffix TEXT,
            floor_type TEXT,
            floor_number TEXT,
            floor_suffix TEXT,
            property_name TEXT,
            street_no_1 TEXT,
            street_no_1_suffix TEXT,
            street_no_2 TEXT,
            street_no_2_suffix TEXT,
            street_number TEXT,
            street_name TEXT,
            street_type TEXT,
            street_suffix TEXT,
            street_full TEXT,
            locality TEXT,
            local_authority TEXT,
            state TEXT,
            address TEXT,
            address_status TEXT,
            address_standard TEXT,
            lotplan_status TEXT,
            address_pid INTEGER,
            geocode_type TEXT,
            latitude REAL,
            longitude REAL
        )
    """
    )

    logger.info("Creating address_current_staging table")
    cursor.execute(
        """
        CREATE TABLE address_current_staging (
            id TEXT,
            lot TEXT,
            plan TEXT,
            unit_type TEXT,
            unit_number TEXT,
            unit_suffix TEXT,
            floor_type TEXT,
            floor_number TEXT,
            floor_suffix TEXT,
            property_name TEXT,
            street_no_1 TEXT,
            street_no_1_suffix TEXT,
            street_no_2 TEXT,
            street_no_2_suffix TEXT,
            street_number TEXT,
            street_name TEXT,
            street_type TEXT,
            street_suffix TEXT,
            street_full TEXT,
            locality TEXT,
            local_authority TEXT,
            state TEXT,
            address TEXT,
            address_status TEXT,
            address_standard TEXT,
            lotplan_status TEXT,
            address_pid INTEGER,
            geocode_type TEXT,
            latitude REAL,
            longitude REAL
        )
    """
    )

    logger.info("Creating address_current table")
    cursor.execute(
        """
        CREATE TABLE address_current (
            id TEXT,
            lot TEXT,
            plan TEXT,
            unit_type TEXT,
            unit_number TEXT,
            unit_suffix TEXT,
            floor_type TEXT,
            floor_number TEXT,
            floor_suffix TEXT,
            property_name TEXT,
            street_no_1 TEXT,
            street_no_1_suffix TEXT,
            street_no_2 TEXT,
            street_no_2_suffix TEXT,
            street_number TEXT,
            street_name TEXT,
            street_type TEXT,
            street_suffix TEXT,
            street_full TEXT,
            locality TEXT,
            local_authority TEXT,
            state TEXT,
            address TEXT,
            address_status TEXT,
            address_standard TEXT,
            lotplan_status TEXT,
            address_pid INTEGER,
            geocode_type TEXT,
            latitude REAL,
            longitude REAL
        )
    """
    )

    logger.info("Creating address_current_loaded table")
    cursor.execute(
        """
        CREATE TABLE address_current_loaded (
            address_pid TEXT,
            loaded BOOLEAN DEFAULT FALSE
        )
        """
    )

    create_metadata_table(cursor)

    cursor.connection.commit()


def create_table_indexes(cursor: sqlite3.Cursor):
    """
    Create the indexes required for the address ETL process
    """
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_address_current_staging_address_pid ON address_current_staging (address_pid)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_geocode_address_pid ON geocode (address_pid)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_address_current_address_pid ON address_current (address_pid)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_address_current_id ON address_current (id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_address_previous_address_pid ON address_previous (address_pid)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_address_previous_id ON address_previous (id)"
    )
    cursor.connection.commit()


def populate_address_current_staging_rows(data: list[dict], cursor: sqlite3.Cursor):
    query = """
        INSERT INTO address_current_staging VALUES(
            :id,
            :lot,
            :plan,
            :unit_type,
            :unit_number,
            :unit_suffix,
            :floor_type,
            :floor_number,
            :floor_suffix,
            :property_name,
            :street_no_1,
            :street_no_1_suffix,
            :street_no_2,
            :street_no_2_suffix,
            :street_number,
            :street_name,
            :street_type,
            :street_suffix,
            :street_full,
            :locality,
            :local_authority,
            :state,
            :address,
            :address_status,
            :address_standard,
            :lotplan_status,
            :address_pid,
            :geocode_type,
            :latitude,
            :longitude
        )
    """
    cursor.executemany(query, data)
    cursor.connection.commit()


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
