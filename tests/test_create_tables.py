import logging
import sqlite3

from address_etl import create_tables


def test_create_tables():
    logger = logging.getLogger(__name__)
    connection = sqlite3.connect(":memory:")
    try:
        cursor = connection.cursor()
        create_tables(cursor, logger)
        assert cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == [
            ("geocode",),
            ("address_previous",),
            ("address_current_staging",),
            ("address_current",),
        ]
    finally:
        connection.close()
