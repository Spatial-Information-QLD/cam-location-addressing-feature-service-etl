from datetime import datetime

from address_etl.time_convert import (
    datetime_to_esri_datetime_utc,
    milliseconds_to_iso_8601_utc,
    utc_to_brisbane_time,
)


def test_milliseconds_to_iso_8601_utc():
    assert milliseconds_to_iso_8601_utc(1749822139000) == datetime.fromisoformat(
        "2025-06-13T13:42:19+00:00"
    )


def test_datetime_to_esri_datetime_utc():
    assert (
        datetime_to_esri_datetime_utc(
            utc_to_brisbane_time(datetime.fromisoformat("2025-06-13T13:42:19+00:00"))
        )
        == "2025-06-13 13:42:19"
    )


def test_utc_to_brisbane_time():
    assert utc_to_brisbane_time(
        datetime.fromisoformat("2025-06-13T13:42:19+00:00")
    ) == datetime.fromisoformat("2025-06-13T23:42:19+10:00")
