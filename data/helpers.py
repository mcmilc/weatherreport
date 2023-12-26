"""Helper functions for retrieving json-file data."""
import os
import json
import tempfile

from weatherreport.config.config import ENV_VAR_NAME
from weatherreport.config.config import weather_report_root

from weatherreport.utilities.filesystem_utils import pjoin
from weatherreport.utilities.filesystem_utils import pisdir
from weatherreport.utilities.filesystem_utils import psplit
from weatherreport.utilities.filesystem_utils import psplitext

access_info_file = pjoin(weather_report_root, "data", "access_info.json")


def read_json_file(filename: str) -> dict:
    """Reads and returns contents of json-file as dict."""
    data = None
    with open(filename, "rb") as fp:
        data = json.load(fp)
    return data


# ACCESS INFO
def get_connection_passwd_info(db_type: str) -> str:
    """Return password for database connection."""
    return read_json_file(access_info_file)[db_type]["passwd"]


def get_database_name_info(db_type):
    """Return database name."""
    return read_json_file(access_info_file)[db_type]["db_name"]


def get_database_access_info(db_type):
    """Return relevant access info for database."""
    return read_json_file(access_info_file)[db_type]


def get_table_name_current_temperature(db_type):
    """Return table info for current temperature table."""
    return read_json_file(access_info_file)[db_type]["tables"]["current"]


def get_table_name_city_type(db_type):
    """Return table info for city type table."""
    return read_json_file(access_info_file)[db_type]["tables"]["city_type"]


def get_table_name_city(db_type):
    """Return table info for city table."""
    return read_json_file(access_info_file)[db_type]["tables"]["city"]


def get_table_name_historical_temperature(db_type):
    """Return table info for historical temperature table."""
    return read_json_file(access_info_file)[db_type]["tables"]["historical"]


def get_weather_api_info():
    """Return relevant info in weather api."""
    return read_json_file(filename=pjoin(weather_report_root, "data", "api_info.json"))


def get_city_info():
    """Return relevant info on cities of interest."""
    return read_json_file(filename=pjoin(weather_report_root, "data", "city_info.json"))


def get_table_info():
    """Return relevant info on tables."""
    return read_json_file(
        filename=pjoin(weather_report_root, "data", "table_info.json")
    )


def get_city_type_info():
    """Return relevant info on city type."""
    return read_json_file(
        filename=pjoin(weather_report_root, "data", "city_type_info.json")
    )


def get_city_id_from_info(city: str):
    """Return city id of city."""
    city_info = get_city_info()
    return city_info[city]["city_id"]


def get_all_city_names():
    """Return all cities registered in info."""
    city_info = pjoin(weather_report_root, "data", "city_info.json")
    return read_json_file(city_info).keys()


def append_suffix_to_filename(filename: str, suffix: str):
    """Append suffix to filename."""
    fn = filename
    if pisdir(filename):
        p, fn = psplit(filename)
        return pjoin(p, fn + suffix)
    return fn + suffix


def generate_temp_filename() -> str:
    """_summary_

    Returns:
        str: full path to temporary file
    """
    _, filename = tempfile.mkstemp(dir=os.environ[ENV_VAR_NAME])
    return filename


def has_file_extension(filename):
    """Returns True is filename has extension."""
    if len(psplitext(filename)[-1]) == 0:
        return False
    return True


def store_as_json_to_tempdir(data: dict, filename: str):
    """Stores data in temporary dir."""
    # if not has_file_extension(filename):
    #    filename = filename + ".json"
    with open(filename, "wb") as fp:
        json.dump(obj=data, fp=fp)


def remove_temporary_file(filename):
    """Remove file."""
    os.remove(filename)
