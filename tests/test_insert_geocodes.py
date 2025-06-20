import sqlite3

import pytest

from address_etl.geocode import insert_geocodes
from address_etl.address_tables import create_tables
from address_etl.sqlite_dict_factory import dict_row_factory


@pytest.fixture
def connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    create_tables(connection.cursor())
    yield connection
    connection.close()


def test_insert_geocodes(connection: sqlite3.Connection):
    cursor = connection.cursor()

    insert_geocodes(
        cursor,
        [
            {
                "attributes": {
                    "objectid": "1",
                    "geocode_type": "1",
                    "address_pid": "1",
                },
                "geometry": {"x": 1.0, "y": 1.0},
            }
        ],
    )

    assert cursor.execute("SELECT * FROM geocode").fetchall() == [
        {
            "address_pid": "1",
            "geocode_id": "1",
            "geocode_type": "1",
            "latitude": 1.0,
            "longitude": 1.0,
        }
    ]

    # geocode_type is now the value "2"
    # insert_geocodes should handle that the objectid already exists
    # and instead update the fields of the row.
    insert_geocodes(
        cursor,
        [
            {
                "attributes": {
                    "objectid": "1",
                    "geocode_type": "2",
                    "address_pid": "1",
                },
                "geometry": {
                    "x": 1.0,
                    "y": 1.0,
                },
            }
        ],
    )

    assert cursor.execute("SELECT * FROM geocode").fetchall() == [
        {
            "address_pid": "1",
            "geocode_id": "1",
            "geocode_type": "2",
            "latitude": 1.0,
            "longitude": 1.0,
        }
    ]
