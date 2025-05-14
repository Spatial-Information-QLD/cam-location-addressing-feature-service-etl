import sqlite3


def test_geocode_join():
    # Create an in-memory SQLite database
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Create tables with the specified schema
    cursor.execute(
        """
        CREATE TABLE address_prev (
            address_pid TEXT,
            address TEXT,
            geocode_type TEXT,
            latitude TEXT,
            longitude TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE address_current_staging (
            address_pid TEXT,
            address TEXT,
            geocode_type TEXT,
            latitude TEXT,
            longitude TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE address_current (
            address_pid TEXT,
            address TEXT,
            geocode_type TEXT,
            latitude TEXT,
            longitude TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE geocode (
            address_pid TEXT,
            geocode_type TEXT,
            latitude TEXT,
            longitude TEXT
        )
    """
    )

    # Insert test data into address_prev
    cursor.executemany(
        """
        INSERT INTO address_prev (address_pid, address, geocode_type, latitude, longitude)
        VALUES (?, ?, ?, ?, ?)
    """,
        [
            ("1", "1 William Street", "PC", "153.5", "-27.2"),
            ("1", "1 William Street", "BC", "153.6", "-27.3"),
        ],
    )

    # Insert test data into address_current_staging
    cursor.execute(
        """
        INSERT INTO address_current_staging (address_pid, address, geocode_type, latitude, longitude)
        VALUES (?, ?, ?, ?, ?)
    """,
        ("1", "1 William Street", None, None, None),
    )

    # Insert test data into geocode
    cursor.executemany(
        """
        INSERT INTO geocode (address_pid, geocode_type, latitude, longitude)
        VALUES (?, ?, ?, ?)
    """,
        [
            ("1", "PC", "153.5", "-27.2"),
            ("1", "BC", "153.6", "-27.3"),
            ("2", "PC", "155.4", "-27.4"),
        ],
    )

    # Perform the left join and insert results into address_current
    cursor.execute(
        """
        INSERT INTO address_current
        SELECT 
            acs.address_pid,
            acs.address,
            g.geocode_type,
            g.latitude,
            g.longitude
        FROM address_current_staging acs
        JOIN geocode g ON acs.address_pid = g.address_pid
    """
    )

    # Verify the results
    cursor.execute("SELECT * FROM address_current ORDER BY address_pid, geocode_type")
    results = cursor.fetchall()

    # Assert that we have exactly 2 rows
    assert len(results) == 2

    # Assert that both rows have address_pid = '1'
    assert all(row[0] == "1" for row in results)

    # Assert the specific values
    assert results[0] == ("1", "1 William Street", "BC", "153.6", "-27.3")
    assert results[1] == ("1", "1 William Street", "PC", "153.5", "-27.2")

    conn.close()
