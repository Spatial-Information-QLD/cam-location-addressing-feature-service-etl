import sqlite3


def create_metadata_table(cursor: sqlite3.Cursor):
    """Create the metadata table.

    The metadata table is used to store the metadata for the ETL run.
    """
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
