import sqlite3

import pytest

from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.id_map import text_to_id_for_pk


@pytest.fixture
def connection():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    connection.execute(
        "CREATE TABLE parcel (parcel_id TEXT, plan_no TEXT, lot_no TEXT)"
    )
    connection.execute(
        "CREATE TABLE parcel_id_map (id INTEGER PRIMARY KEY AUTOINCREMENT, iri TEXT UNIQUE)"
    )
    yield connection
    connection.close()


def test_empty_map_table(connection: sqlite3.Connection):
    cursor = connection.cursor()
    # 10 parcel rows.
    parcel_data = [
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/10SP149947",
            "SP149947",
            "10",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/8SP190767",
            "SP190767",
            "8",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/38SP195511",
            "SP195511",
            "38",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/483RP851228",
            "RP851228",
            "483",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/900SP244015",
            "SP244015",
            "900",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/491RP137509",
            "RP137509",
            "491",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/7406SP207481",
            "SP207481",
            "7406",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/1SP171353",
            "SP171353",
            "1",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/3SP100761",
            "SP100761",
            "3",
        ),
        ("https://linked.data.gov.au/dataset/qld-addr/parcel/5BUP5278", "BUP5278", "5"),
    ]

    cursor.executemany(
        "INSERT INTO parcel (parcel_id, plan_no, lot_no) VALUES (?, ?, ?)", parcel_data
    )
    connection.commit()

    text_to_id_for_pk("parcel_id_map", "parcel", "parcel_id", cursor)

    # Check that there are 10 entries in parcel_id_map now.
    cursor.execute("SELECT COUNT(*) as count FROM parcel_id_map")
    assert cursor.fetchone()["count"] == 10

    # Check that the parcel table has been updated.
    cursor.execute(
        """
        SELECT parcel.parcel_id, parcel_id_map.id
        FROM parcel
        LEFT JOIN parcel_id_map ON parcel.parcel_id = parcel_id_map.id
        WHERE parcel_id_map.id IS NULL
        """
    )
    results = cursor.fetchall()
    assert len(results) == 0

    # All parcel_id values should be the same as the id in the map table.
    # All iri values in the map table should start with http.
    # Result size should be 10.
    cursor.execute(
        """
        SELECT parcel.parcel_id, parcel_id_map.id, parcel_id_map.iri
        FROM parcel
        LEFT JOIN parcel_id_map ON parcel.parcel_id = parcel_id_map.id
        """
    )
    results = cursor.fetchall()
    assert len(results) == 10
    for row in results:
        assert row["parcel_id"] == row["id"]
        assert row["iri"].startswith("http")


def test_map_table_with_some_values(connection: sqlite3.Connection):
    cursor = connection.cursor()
    parcel_data = [
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/10SP149947",
            "SP149947",
            "10",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/8SP190767",
            "SP190767",
            "8",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/38SP195511",
            "SP195511",
            "38",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/483RP851228",
            "RP851228",
            "483",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/900SP244015",
            "SP244015",
            "900",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/491RP137509",
            "RP137509",
            "491",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/7406SP207481",
            "SP207481",
            "7406",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/1SP171353",
            "SP171353",
            "1",
        ),
        (
            "https://linked.data.gov.au/dataset/qld-addr/parcel/3SP100761",
            "SP100761",
            "3",
        ),
        ("https://linked.data.gov.au/dataset/qld-addr/parcel/5BUP5278", "BUP5278", "5"),
    ]
    cursor.executemany(
        "INSERT INTO parcel_id_map (iri) VALUES (?)",
        [(row[0],) for row in parcel_data[:2]],
    )
    cursor.executemany(
        "INSERT INTO parcel (parcel_id, plan_no, lot_no) VALUES (?, ?, ?)",
        parcel_data,
    )
    connection.commit()

    # Two values in the map table as a starting point.
    cursor.execute("SELECT COUNT(*) as count FROM parcel_id_map")
    assert cursor.fetchone()["count"] == 2

    # All 10 values in the parcel table.
    cursor.execute("SELECT COUNT(*) as count FROM parcel")
    assert cursor.fetchone()["count"] == 10

    text_to_id_for_pk("parcel_id_map", "parcel", "parcel_id", cursor)

    # Now 10 values in the map table.
    cursor.execute("SELECT COUNT(*) as count FROM parcel_id_map")
    assert cursor.fetchone()["count"] == 10

    # Check that the parcel table has been updated.
    cursor.execute(
        """
        SELECT parcel.parcel_id, parcel_id_map.id
        FROM parcel
        LEFT JOIN parcel_id_map ON parcel.parcel_id = parcel_id_map.id
        WHERE parcel_id_map.id IS NULL
        """
    )
    results = cursor.fetchall()
    assert len(results) == 0

    # All parcel_id values should be the same as the id in the map table.
    # All iri values in the map table should start with http.
    # Result size should be 10.
    cursor.execute(
        """
        SELECT parcel.parcel_id, parcel_id_map.id, parcel_id_map.iri
        FROM parcel
        LEFT JOIN parcel_id_map ON parcel.parcel_id = parcel_id_map.id
        """
    )
    results = cursor.fetchall()
    assert len(results) == 10
    for row in results:
        assert row["parcel_id"] == row["id"]
        assert row["iri"].startswith("http")
