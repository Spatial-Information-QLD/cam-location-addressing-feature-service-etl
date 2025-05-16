import sqlite3


def dict_row_factory(cursor: sqlite3.Cursor, row: sqlite3.Row) -> dict:
    """
    Convert a sqlite3.Row to a dictionary.

    See https://docs.python.org/3/library/sqlite3.html#how-to-create-and-use-row-factories.
    """
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}
