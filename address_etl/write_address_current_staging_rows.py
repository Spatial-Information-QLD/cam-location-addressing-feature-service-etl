import sqlite3


def write_address_current_staging_rows(data: list[dict], cursor: sqlite3.Cursor):
    query = """
        INSERT INTO address_current_staging VALUES(
            :id,
            :lot,
            :plan,
            :unit_type,
            :unit_number,
            :unit_suffix,
            :floor_type,
            :floor_number,
            :floor_suffix,
            :property_name,
            :street_no_1,
            :street_no_1_suffix,
            :street_no_2,
            :street_no_2_suffix,
            :street_number,
            :street_name,
            :street_type,
            :street_suffix,
            :street_full,
            :locality,
            :local_authority,
            :state,
            :address,
            :address_status,
            :address_standard,
            :lotplan_status,
            :address_pid,
            :geocode_type,
            :latitude,
            :longitude
        )
    """
    cursor.executemany(query, data)
    cursor.connection.commit()
