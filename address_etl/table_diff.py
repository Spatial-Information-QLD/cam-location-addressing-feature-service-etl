import sqlite3


def get_deleted_rows(hash_column_name: str, id_column_name: str, table_previous: str, table_current: str, cursor: sqlite3.Cursor):
    cursor.execute(
        f"""
        SELECT DISTINCT p.{id_column_name}
        FROM {table_previous} p
        LEFT JOIN {table_current} c ON p.{hash_column_name} = c.{hash_column_name}
        WHERE c.{hash_column_name} IS NULL
        """
    )
    rows_deleted = cursor.fetchall()
    return set([row[id_column_name] for row in rows_deleted])


def get_added_rows(hash_column_name: str, id_column_name: str, table_previous: str, table_current: str, cursor: sqlite3.Cursor):
    cursor.execute(
        f"""
        SELECT DISTINCT c.{id_column_name}
        FROM {table_current} c
        LEFT JOIN {table_previous} p ON c.{hash_column_name} = p.{hash_column_name}
        WHERE p.{hash_column_name} IS NULL
        """
    )
    rows_added = cursor.fetchall()
    return set([row[id_column_name] for row in rows_added])


def compute_table_diff(
    hash_column_name: str,
    id_column_name: str,
    table_previous: str,
    table_current: str,
    cursor: sqlite3.Cursor,
) -> tuple[set[str], set[str]]:
    """
    Compute the difference between two tables by comparing the value of a column.

    See hash_rows_in_table function to create a hash of each row in a table.
    """
    rows_deleted = get_deleted_rows(hash_column_name, id_column_name, table_previous, table_current, cursor)
    rows_added = get_added_rows(hash_column_name, id_column_name, table_previous, table_current, cursor)
    return rows_deleted, rows_added
