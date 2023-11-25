import os
import json
import datetime as dt
import numpy as np

pjoin = os.path.join


def build_date(year: int, month: int, day: int) -> str:
    return dt.date(year=year, month=month, day=day).strftime("%Y-%m-%d")


def read_json(filename: str) -> dict:
    return json.load(open(filename))


def round_val(value: float) -> np.int32:
    return int(np.round(value))


def generate_uuid(s_time: str, city_id: int = 1) -> int:
    """Generates uuid based on absolute time in int format

    Args:
        s_time (str): timestamp from weatherAPI in yyyy-mm-ddThh:mm format

    Returns:
        int: absolute time
    """
    dt_time = dt.datetime.strptime(s_time, "%Y-%m-%dT%H:%M")
    return round_val(dt_time.timestamp()) + int(city_id)
