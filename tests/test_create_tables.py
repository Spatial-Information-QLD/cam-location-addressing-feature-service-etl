import sqlite3

from address_etl.address_tables import create_tables


def test_create_tables():
    connection = sqlite3.connect(":memory:")
    try:
        cursor = connection.cursor()
        create_tables(cursor)
        assert cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == [
            ("geocode",),
            ("address_previous",),
            ("address_current_staging",),
            ("address_current",),
            ("address_current_loaded",),
            ("metadata",),
        ]
    finally:
        connection.close()
