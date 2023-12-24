"""Conversion functions"""
import datetime as dt
import numpy as np


def convert_timestamp(timestamp: str) -> str:
    """Remove T from UTC timeformat for SQL-type database timestamp formats."""
    return str.replace(timestamp, "T", " ") + ":00"


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
