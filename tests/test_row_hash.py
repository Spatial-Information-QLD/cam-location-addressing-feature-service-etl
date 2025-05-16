import sqlite3

from address_etl.create_row_hash import create_row_hash, hash_rows_in_table
from address_etl.sqlite_dict_factory import dict_row_factory


def test_create_row_hash():
    row = {"rowid": 1, "a": 1, "b": 2, "c": 3}
    assert create_row_hash(row) == "a80482d74631d666f097f2da3bccc534"
    row2 = {"a": 1, "b": 2, "c": 3}
    assert create_row_hash(row2) == "a80482d74631d666f097f2da3bccc534"


def test_hash_rows_in_table():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = dict_row_factory
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id TEXT, a INTEGER, b INTEGER, c INTEGER)")
    cursor.execute("INSERT INTO test (a, b, c) VALUES (1, 2, 3)")
    hash_rows_in_table("test", cursor)
    assert (
        cursor.execute("SELECT id FROM test").fetchone()["id"]
        == "a80482d74631d666f097f2da3bccc534"
    )
