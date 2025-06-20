import sqlite3
from datetime import datetime

import pytz

from address_etl.time_convert import utc_to_brisbane_time


def metadata_write_start_time(cursor: sqlite3.Cursor):
    """Write the start time to the metadata table"""
    current_datetime = datetime.now(pytz.UTC)
    brisbane_time = utc_to_brisbane_time(current_datetime)
    cursor.execute(
        "INSERT INTO metadata (start_time) VALUES (?)",
        (brisbane_time.strftime("%Y-%m-%dT%H:%M:%S%z"),),
    )
    cursor.connection.commit()


def metadata_write_end_time(cursor: sqlite3.Cursor, end_time_str: str | None = None):
    """Write the end time to the metadata table"""
    if end_time_str is None:
        end_time = datetime.now(pytz.UTC)
        brisbane_time = utc_to_brisbane_time(end_time)
        end_time_str = brisbane_time.strftime("%Y-%m-%dT%H:%M:%S%z")

    cursor.execute(
        "UPDATE metadata SET end_time = ? WHERE id = 1",
        (end_time_str,),
    )
    cursor.connection.commit()
