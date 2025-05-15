import logging
import sqlite3

logger = logging.getLogger(__name__)


def create_tables(cursor: sqlite3.Cursor):
    """
    Create the tables required for the address ETL process

    The tables are:

    - geocode - the table to store the geocode data pulled from SIRRTE.
    - address_previous - the table to store the location address table from the previous ETL run.
    - address_current_staging - the table to store the generated location address data from the current ETL run.
    - address_current - the final location address table. This table contains the row hash to perform the diffing algorithm with the previous ETL run.
    """

    logger.info("Creating geocode table")
    cursor.execute(
        """
        CREATE TABLE geocode (
            address_pid INTEGER,
            geocode_type TEXT,
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
    cursor.connection.commit()
