import json

from weatherreport.utilities.helpers import pjoin
from weatherreport.config.config import sbw_root

access_info_file = pjoin(sbw_root, "data", "access.json")


def read_json(filename: str) -> dict:
    return json.load(open(filename))


def get_connection_passwd(db_type):
    return read_json(access_info_file)[db_type]["passwd"]


def get_connection_database(db_type):
    return read_json(access_info_file)[db_type]["db_name"]


def get_access_info(db_type):
    return read_json(access_info_file)[db_type]


def get_api_info():
    return read_json(filename=pjoin(sbw_root, "data", "api_info.json"))


def get_city_info():
    return read_json(filename=pjoin(sbw_root, "data", "city_info.json"))


def get_current_temperature_table(db_type):
    return read_json(access_info_file)[db_type]["tables"]["current"]


def get_city_type_table(db_type):
    return read_json(access_info_file)[db_type]["tables"]["city_type"]


def get_city_table(db_type):
    return read_json(access_info_file)[db_type]["tables"]["city"]


def get_historical_temperature_table(db_type):
    return read_json(access_info_file)[db_type]["tables"]["historical"]


def get_table_info():
    return read_json(filename=pjoin(sbw_root, "data", "table_info.json"))


def get_city_type_info():
    return read_json(filename=pjoin(sbw_root, "data", "city_type_info.json"))


def get_city_id_from_info(city: str):
    city_info = get_city_info()
    return city_info[city]["city_id"]
