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
        SELECT rowid, {pk_column_name}
        FROM {table_name}
        WHERE {pk_column_name} NOT IN (
            SELECT iri FROM {map_table_name}
            UNION
            SELECT id FROM {map_table_name}
        );
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
    cursor.execute(
        f"SELECT {table_name}.rowid FROM {table_name} LEFT JOIN {map_table_name} ON {table_name}.{pk_column_name} = {map_table_name}.id WHERE {map_table_name}.id IS NULL"
    )
    results = cursor.fetchall()
    total_rows = len(results)
    batch_size = 10000

    for offset in range(0, total_rows, batch_size):
        logger.info(
            f"Processing batch {offset//batch_size + 1} of {(total_rows + batch_size - 1)//batch_size}"
        )

        query = f"""
            UPDATE {table_name}
            SET {pk_column_name} = (
                SELECT id 
                FROM {map_table_name} 
                WHERE iri = {table_name}.{pk_column_name}
            )
            WHERE {table_name}.rowid IN ({', '.join(str(row["rowid"]) for row in results[offset:offset+batch_size])})
        """
        cursor.execute(query)
        cursor.connection.commit()

    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def text_to_id_for_pk_migrate_column(
    table_name: str,
    pk_column_name: str,
    cursor: sqlite3.Cursor,
):

    # Get all existing indexes before dropping the table
    logger.info(f"Getting existing indexes for {table_name}")
    cursor.execute(
        f"SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='{table_name}'"
    )
    indexes = cursor.fetchall()

    # Get the table info to check for PRIMARY KEY
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_info = cursor.fetchall()
    pk_info = next((col for col in table_info if col["pk"] > 0), None)

    # Create a new table, changing the id column of the focus table to be of type INTEGER.
    logger.info(f"Creating new table {table_name}_new")
    cursor.execute(
        f"""
        CREATE TABLE {table_name}_new (
            {pk_column_name} INTEGER{' PRIMARY KEY' if pk_info and pk_info['name'] == pk_column_name else ''},
            {', '.join(f"{col['name']} {col['type']}" for col in table_info if col['name'] != pk_column_name)}
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

    # Recreate all indexes
    logger.info(f"Recreating indexes for {table_name}")

    # First, create the PRIMARY KEY index if needed
    if pk_info and pk_info["name"] == pk_column_name:
        logger.info(f"Creating PRIMARY KEY index for {table_name}")
        cursor.execute(
            f"CREATE UNIQUE INDEX idx_{table_name}_{pk_column_name} ON {table_name} ({pk_column_name})"
        )

    # Then recreate all other indexes
    for index in indexes:
        if index["sql"] is not None:
            # Replace the old table name with the new one in the index creation SQL
            index_sql = index["sql"].replace(f"ON {table_name}", f"ON {table_name}")
            cursor.execute(index_sql)
        else:
            # For indexes without SQL (like auto-generated ones), we'll need to recreate them based on the table structure
            # Get the index info to determine if it's unique
            cursor.execute(f"PRAGMA index_info('{index['name']}')")
            index_info = cursor.fetchall()
            if index_info:
                # Get the columns in the index
                columns = []
                for info in index_info:
                    cursor.execute(f"PRAGMA table_info('{table_name}')")
                    table_info = cursor.fetchall()
                    if info["cid"] < len(table_info):
                        columns.append(table_info[info["cid"]]["name"])

                if columns:
                    # Check if it's a unique index
                    cursor.execute(f"PRAGMA index_list('{table_name}')")
                    index_list = cursor.fetchall()
                    is_unique = any(
                        idx["name"] == index["name"] and idx["unique"]
                        for idx in index_list
                    )

                    # Recreate the index
                    unique_str = "UNIQUE " if is_unique else ""
                    cursor.execute(
                        f"CREATE {unique_str}INDEX {index['name']} ON {table_name} ({', '.join(columns)})"
                    )

    cursor.connection.commit()
