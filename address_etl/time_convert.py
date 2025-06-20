import pytz

from datetime import datetime


def milliseconds_to_iso_8601_utc(milliseconds: int) -> datetime:
    """Convert milliseconds since epoch to ISO 8601 UTC string"""
    return datetime.fromtimestamp(milliseconds / 1000, tz=pytz.UTC)


def datetime_to_esri_datetime_utc(value: datetime) -> str:
    """Convert datetime to ESRI date string in UTC"""
    return value.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")


def utc_to_brisbane_time(utc_datetime: datetime) -> datetime:
    """Convert UTC time to Brisbane time"""
    brisbane_dt = utc_datetime.astimezone(pytz.timezone("Australia/Brisbane"))
    return brisbane_dt
