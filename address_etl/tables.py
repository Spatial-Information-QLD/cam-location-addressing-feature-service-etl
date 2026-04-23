import logging
import sqlite3

logger = logging.getLogger(__name__)


def create_metadata_table(cursor: sqlite3.Cursor):
    """Create the metadata table.

    The metadata table is used to store the metadata for the ETL run.
    """
    logger.info("Creating metadata table")
    cursor.execute(
        """
        CREATE TABLE metadata (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            start_time TEXT,
            end_time TEXT
        )
    """
    )
    cursor.connection.commit()


def create_geocode_type_code_table(cursor: sqlite3.Cursor):
    """Create the geocode type code table.

    This table stores feature-service geocode type IRIs and their resolved
    legacy SIR-PUB codes so later ETL runs can reuse them without depending on
    the SPARQL endpoint being available.
    """
    logger.info("Creating geocode_type_code table")
    cursor.execute(
        """
        CREATE TABLE geocode_type_code (
            geocode_type_iri TEXT PRIMARY KEY,
            geocode_type_code TEXT NOT NULL
        )
    """
    )
    cursor.connection.commit()


def create_address_iri_pid_map_table(cursor: sqlite3.Cursor):
    """Create the persisted address IRI to PID cache table."""
    logger.info("Creating address_iri_pid_map table")
    cursor.execute(
        """
        CREATE TABLE address_iri_pid_map (
            address_iri TEXT PRIMARY KEY,
            address_pid TEXT NOT NULL
        )
    """
    )
    cursor.execute(
        """
        CREATE INDEX idx_address_iri_pid_map_address_pid
        ON address_iri_pid_map (address_pid)
        """
    )
    cursor.connection.commit()
