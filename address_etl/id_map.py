import time
import sqlite3
import logging

logger = logging.getLogger(__name__)


def text_to_id_for_pk(
    map_table_name: str,
    table_name: str,
    pk_column_name: str,
    cursor: sqlite3.Cursor,
):
    """
    This function takes a focus table (table_name) and updates all values to an integer that exists
    in the map table (map_table_name).

    For all identifiers in the focus table that do not exist in the map table, insert it into
    the map table.

    Update the focus table to have the integer value of the map table.

    The map table is expected to be of the following form:
        - `CREATE TABLE entity_id_map (id INTEGER PRIMARY KEY AUTOINCREMENT, iri TEXT UNIQUE)`
    """

    start_time = time.time()

    logger.info(
        f"Mapping table {table_name} column {pk_column_name} to the id in {map_table_name}"
    )

    # Insert new identifiers into the map table.
    logger.info(f"Inserting new identifiers into {map_table_name}")

    cursor.execute(
        f"""
        SELECT {pk_column_name}
        FROM {table_name}
        LEFT JOIN {map_table_name} ON {table_name}.{pk_column_name} = {map_table_name}.iri
        WHERE {map_table_name}.iri IS NULL
        """
    )
    results = cursor.fetchall()

    logger.info(f"Total new identifiers to insert: {len(results)}")
    cursor.executemany(
        f"""
        INSERT INTO {map_table_name} (iri) VALUES (?)
        """,
        [(row[pk_column_name],) for row in results],
    )
    cursor.connection.commit()

    # Update the focus table to have the integer value of the map table
    logger.info(f"Updating {table_name} to have the integer value of {map_table_name}")

    cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
    total_rows = cursor.fetchone()["count"]
    batch_size = 10000

    for offset in range(0, total_rows, batch_size):
        logger.info(
            f"Processing batch {offset//batch_size + 1} of {(total_rows + batch_size - 1)//batch_size}"
        )
        cursor.execute(
            f"""
            UPDATE {table_name}
            SET {pk_column_name} = (
                SELECT id 
                FROM {map_table_name} 
                WHERE iri = {table_name}.{pk_column_name}
            )
            WHERE rowid IN (
                SELECT rowid 
                FROM {table_name} 
                LIMIT {batch_size} 
                OFFSET {offset}
            )
            """
        )
        cursor.connection.commit()

    # Create a new table, changing the id column of the focus table to be of type INTEGER.
    logger.info(f"Creating new table {table_name}_new")
    cursor.execute(
        f"""
        CREATE TABLE {table_name}_new (
            {pk_column_name} INTEGER UNIQUE,
            {', '.join(f"{col['name']} {col['type']}" for col in cursor.execute(f"PRAGMA table_info({table_name})").fetchall() if col['name'] != pk_column_name)}
        )
        """
    )

    # Copy data from old table to new table.
    logger.info(f"Copying data from {table_name} to {table_name}_new")
    cursor.execute(
        f"""
        INSERT INTO {table_name}_new
        SELECT * FROM {table_name}
        """
    )

    # Drop the old table.
    logger.info(f"Dropping old table {table_name}")
    cursor.execute(f"DROP TABLE {table_name}")

    # Rename the new table to the old table's name.
    logger.info(f"Renaming {table_name}_new to {table_name}")
    cursor.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")

    cursor.connection.commit()

    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")
