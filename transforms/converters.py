"""Conversion functions"""
import datetime as dt
import numpy as np
import pytz


TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def convert_timestamp_from_utc(timestamp: dt.datetime, timezone: str):
    new_tz = pytz.timezone(timezone)
    return timestamp.astimezone(new_tz)


def convert_timestamp_to_utc(timestamp: str, timezone: str) -> str:
    """Remove T from UTC timeformat for SQL-type database timestamp formats."""
    timestamp = str.replace(timestamp, "T", " ") + ":00"
    dt_timestamp = dt.datetime.strptime(timestamp, TIME_FORMAT)
    tz = pytz.timezone(timezone)
    dt_timestamp = tz.localize(dt_timestamp)
    utc_tz = pytz.timezone("utc")
    new_dt_timestamp = dt_timestamp.astimezone(utc_tz)
    return new_dt_timestamp.strftime(TIME_FORMAT)


def round_float_to_int(value: float) -> int:
    """Round floating point value to integer."""
    return int(np.round(value))


def generate_uuid(s_time: str, city_id: int = 1) -> int:
    """Generates uuid based on absolute time in int format

    Args:
        s_time (str): timestamp from weatherAPI in yyyy-mm-ddThh:mm format

    Returns:
        int: absolute time
    """
    dt_time = dt.datetime.strptime(s_time, "%Y-%m-%dT%H:%M")
    return round_float_to_int(dt_time.timestamp()) + int(city_id)
