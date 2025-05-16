import sqlite3


def get_deleted_rows(table_previous: str, table_current: str, cursor: sqlite3.Cursor):
    cursor.execute(
        f"""
        SELECT p.*
        FROM {table_previous} p
        LEFT JOIN {table_current} c ON p.id = c.id
        WHERE c.id IS NULL
        """
    )
    rows_deleted = cursor.fetchall()
    return rows_deleted


def get_added_rows(table_previous: str, table_current: str, cursor: sqlite3.Cursor):
    cursor.execute(
        f"""
        SELECT c.*
        FROM {table_current} c
        LEFT JOIN {table_previous} p ON c.id = p.id
        WHERE p.id IS NULL
        """
    )
    rows_added = cursor.fetchall()
    return rows_added


def compute_table_diff(
    table_previous: str, table_current: str, cursor: sqlite3.Cursor
) -> tuple[list[dict], list[dict]]:
    """
    Compute the difference between two tables.
    """
    rows_deleted = get_deleted_rows(table_previous, table_current, cursor)
    rows_added = get_added_rows(table_previous, table_current, cursor)
    return rows_deleted, rows_added
