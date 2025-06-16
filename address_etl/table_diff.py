import sqlite3


def get_deleted_rows(table_previous: str, table_current: str, cursor: sqlite3.Cursor):
    cursor.execute(
        f"""
        SELECT DISTINCT p.address_pid
        FROM {table_previous} p
        LEFT JOIN {table_current} c ON p.id = c.id
        WHERE c.id IS NULL
        """
    )
    rows_deleted = cursor.fetchall()
    return set([row["address_pid"] for row in rows_deleted])


def get_added_rows(table_previous: str, table_current: str, cursor: sqlite3.Cursor):
    cursor.execute(
        f"""
        SELECT DISTINCT c.address_pid
        FROM {table_current} c
        LEFT JOIN {table_previous} p ON c.id = p.id
        WHERE p.id IS NULL
        """
    )
    rows_added = cursor.fetchall()
    return set([row["address_pid"] for row in rows_added])


def compute_table_diff(
    table_previous: str, table_current: str, cursor: sqlite3.Cursor
) -> tuple[set[str], set[str]]:
    """
    Compute the difference between two tables.
    """
    rows_deleted = get_deleted_rows(table_previous, table_current, cursor)
    rows_added = get_added_rows(table_previous, table_current, cursor)
    return rows_deleted, rows_added
