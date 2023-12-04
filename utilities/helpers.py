import os
import json
import datetime as dt
import numpy as np

# CONFIG
from weatherreport.config.config import sbw_root

pjoin = os.path.join
file_exists = os.path.exists


def build_date(year: int, month: int, day: int) -> str:
    return dt.date(year=year, month=month, day=day).strftime("%Y-%m-%d")


def read_json(filename: str) -> dict:
    return json.load(open(filename))


def round_val(value: float) -> int:
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


def get_connection_passwd(db_type):
    return read_json(pjoin(sbw_root, "data", "access.json"))[db_type]["passwd"]


def get_connection_database(db_type):
    return read_json(pjoin(sbw_root, "data", "access.json"))[db_type]["db_name"]


def get_access_info(db_type):
    return read_json(pjoin(sbw_root, "data", "access.json"))[db_type]


def get_api_info():
    return read_json(filename=pjoin(sbw_root, "data", "api_info.json"))


def get_city_info():
    return read_json(filename=pjoin(sbw_root, "data", "city_info.json"))


def setup_bigquery_environment(service_account_file):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = pjoin(
        sbw_root,
        "database",
        service_account_file,
    )


def get_table_info():
    return read_json(filename=pjoin(sbw_root, "data", "table_info.json"))


def get_city_type_info():
    return read_json(filename=pjoin(sbw_root, "data", "city_type_info.json"))


def get_city_id(city: str):
    city_info = get_city_info()
    return city_info[city]["city_id"]
