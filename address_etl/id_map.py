import sqlite3


def text_to_id(
    map_table_name: str,
    table_name: str,
    id_column_name: str,
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

    # Insert new identifiers into the map table.
    cursor.execute(
        f"""
        INSERT INTO {map_table_name} (iri)
        SELECT DISTINCT {id_column_name}
        FROM {table_name}
        WHERE {id_column_name} NOT IN (SELECT iri FROM {map_table_name})
        """
    )

    # Update the focus table to have the integer value of the map table
    cursor.execute(
        f"""
        UPDATE {table_name}
        SET {id_column_name} = (SELECT id FROM {map_table_name} WHERE iri = {id_column_name})
        """
    )

    # Create a new table, changing the id column of the focus table to be of type INTEGER.
    cursor.execute(
        f"""
        CREATE TABLE {table_name}_new (
            {id_column_name} INTEGER UNIQUE,
            {', '.join(f"{col['name']} {col['type']}" for col in cursor.execute(f"PRAGMA table_info({table_name})").fetchall() if col['name'] != id_column_name)}
        )
        """
    )

    # Copy data from old table to new table.
    cursor.execute(
        f"""
        INSERT INTO {table_name}_new
        SELECT * FROM {table_name}
        """
    )

    # Drop the old table.
    cursor.execute(f"DROP TABLE {table_name}")

    # Rename the new table to the old table's name.
    cursor.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
