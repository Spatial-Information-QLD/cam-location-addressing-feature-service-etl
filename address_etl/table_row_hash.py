import hashlib
import logging
import sqlite3

from address_etl.sqlite_dict_factory import dict_row_factory

logger = logging.getLogger(__name__)


def create_row_hash(row: dict) -> str:
    """
    Create a hash of a dictionary row.

    The row is converted to a single line string with key=value pairs,
    excluding the rowid and the id column.

    The string is then hashed using the blake2b algorithm, resulting in a 16-byte hash.
    """
    single_line_row = "".join(
        f"{key}={value}" for key, value in row.items() if key not in ("rowid", "id")
    )
    return hashlib.blake2b(single_line_row.encode(), digest_size=16).hexdigest()


def hash_rows_in_table(table_name: str, cursor: sqlite3.Cursor):
    """
    Create a hash of each row in the table.

    The hash is stored in the id column.
    """
    logger.info(f"Hashing rows in table {table_name}")
    cursor.connection.row_factory = dict_row_factory
    update_cursor = cursor.connection.cursor()

    try:
        for row in cursor.execute(f"SELECT rowid, * FROM {table_name}"):
            row_hash = create_row_hash(row)
            update_cursor.execute(
                f"UPDATE {table_name} SET id = ? WHERE rowid = ?",
                (row_hash, row["rowid"]),
            )
    finally:
        cursor.connection.commit()
        update_cursor.close()
