import sqlite3

from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.table_diff import compute_table_diff


def _create_tables(cursor: sqlite3.Cursor):
    cursor.execute("CREATE TABLE previous (id TEXT, name TEXT, type TEXT)")
    cursor.execute("CREATE TABLE current (id TEXT, name TEXT, type TEXT)")


def _insert_rows(cursor: sqlite3.Cursor, table: str, rows: list[dict]):
    cursor.executemany(
        f"INSERT INTO {table} (id, name, type) VALUES (:id, :name, :type)", rows
    )


def test_get_table_diff_no_changes():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    cursor = connection.cursor()
    _create_tables(cursor)
    _insert_rows(cursor, "previous", [{"id": "1", "name": "John", "type": "person"}])
    _insert_rows(cursor, "current", [{"id": "1", "name": "John", "type": "person"}])
    rows_deleted, rows_added = compute_table_diff("previous", "current", cursor)
    assert rows_deleted == []
    assert rows_added == []


def test_get_table_diff_rows_deleted():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    cursor = connection.cursor()
    _create_tables(cursor)
    _insert_rows(
        cursor,
        "previous",
        [
            {"id": "1", "name": "John", "type": "person"},
            {"id": "2", "name": "John2", "type": "person"},
        ],
    )
    _insert_rows(cursor, "current", [{"id": "2", "name": "John2", "type": "person"}])
    rows_deleted, rows_added = compute_table_diff("previous", "current", cursor)
    assert rows_deleted == [{"id": "1", "name": "John", "type": "person"}]
    assert rows_added == []


def test_get_table_diff_rows_added():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    cursor = connection.cursor()
    _create_tables(cursor)
    _insert_rows(cursor, "previous", [{"id": "1", "name": "John", "type": "person"}])
    _insert_rows(
        cursor,
        "current",
        [
            {"id": "1", "name": "John", "type": "person"},
            {"id": "2", "name": "John2", "type": "person"},
        ],
    )
    rows_deleted, rows_added = compute_table_diff("previous", "current", cursor)
    assert rows_deleted == []
    assert rows_added == [{"id": "2", "name": "John2", "type": "person"}]


def test_get_table_diff_rows_deleted_and_added():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    cursor = connection.cursor()
    _create_tables(cursor)
    _insert_rows(cursor, "previous", [{"id": "1", "name": "John", "type": "person"}])
    _insert_rows(cursor, "current", [{"id": "2", "name": "John2", "type": "person"}])
    rows_deleted, rows_added = compute_table_diff("previous", "current", cursor)
    assert rows_deleted == [{"id": "1", "name": "John", "type": "person"}]
    assert rows_added == [{"id": "2", "name": "John2", "type": "person"}]
