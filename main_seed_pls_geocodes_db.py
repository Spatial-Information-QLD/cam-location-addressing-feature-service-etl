"""
Open pls.db and create a connection.

Open address-with-geocodes.db and attach it to the connection.

Load the geocodes data into pls.db's lf_geocode_sp_survey_point table.
"""

import sqlite3

with sqlite3.connect("pls.db") as conn:
    cursor = conn.cursor()
    cursor.execute("ATTACH DATABASE 'address-with-geocodes.db' AS geocodes")
    cursor.execute(
        """
        INSERT INTO lf_geocode_sp_survey_point
        SELECT
            geocode_id,
            geocode_type,
            address_pid,
            NULL,
            latitude,
            longitude,
            NULL
        FROM geocodes.geocode
        """
    )
