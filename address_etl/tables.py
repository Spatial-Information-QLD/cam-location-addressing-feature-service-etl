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
