from weatherreport.config.config import sbw_root
from weatherreport.utilities.helpers import pjoin
from weatherreport.utilities.helpers import read_json
from weatherreport.utilities.helpers import get_table_info
from weatherreport.utilities.helpers import get_access_info

access_info = pjoin(sbw_root, "data", "access.json")


def append_bigquery_prefix_to_table(db_type, table_name):
    if db_type == "bigquery":
        access_info = get_access_info("bigqiery")
        project_id = access_info["project_id"]
        dataset = access_info["db_name"]
        return project_id + "." + dataset + "." + table_name
    else:
        return table_name


def add_historical_temperature_query(db_type):
    historical_table = get_historical_table(db_type)
    return (
        f"INSERT INTO {historical_table} (historical_temperature_id, city_id, time_measured, temperature) "
        f"VALUES (%(historical_temperature_id)s, %(city_id)s, TIMESTAMP('%(time_measured)s'), %(temperature)s)"
    )


def get_current_table(db_type):
    return read_json(access_info)[db_type]["tables"]["current"]


def get_city_type_table(db_type):
    return read_json(access_info)[db_type]["tables"]["city_type"]


def get_city_table(db_type):
    return read_json(access_info)[db_type]["tables"]["city"]


def get_historical_table(db_type):
    return read_json(access_info)[db_type]["tables"]["historical"]


def add_current_temperature_query(db_type):
    current_table = get_current_table(db_type)
    return (
        f"INSERT INTO {current_table} (city_id, time_measured, temperature) "
        f"VALUES (%(city_id)s, TIMESTAMP('%(time_measured)s'), %(temperature)s)"
    )


def add_city_type_query(db_type):
    city_type_table = get_city_type_table(db_type)
    return (
        f"INSERT INTO {city_type_table} (city_type_id, name) "
        f"VALUES (%(city_type_id)s, '%(name)s')"
    )


def add_city_query(db_type):
    city_table = get_city_table(db_type)
    return (
        f"INSERT INTO {city_table} (city_id, name, city_type_id, longitude, latitude) "
        f"VALUES (%(city_id)s, '%(name)s', %(city_type_id)s, %(longitude)s, %(latitude)s)"
    )


def get_all_city_names():
    city_info = pjoin(sbw_root, "data", "city_info.json")
    return read_json(city_info).keys()


def get_all_max_historical_temperatures_query(db_type):
    historical_table = get_historical_table(db_type)
    city_table = get_city_table(db_type)
    return (
        f"SELECT "
        f"  MAX({historical_table}.temperature) AS MAX_temperature, "
        f"  {city_table}.name "
        f"FROM {historical_table} "
        f"JOIN city ON {historical_table}.city_id = {city_table}.city_id "
        f"GROUP BY {city_table}.name;"
    )


def get_max_historical_temperature_timestamps_query(
    city: str, temperature: int, db_type: str
):
    historical_table = get_historical_table(db_type)
    return (
        f"SELECT time_measured "
        f"FROM {historical_table} "
        f"JOIN city ON {historical_table}.city_id = city.city_id "
        f"WHERE city.name = '{city}' AND {historical_table}.temperature = {temperature}"
    )


def get_max_historical_temperature_for_city_query(city: int, db_type: str):
    historical_table = get_historical_table(db_type)
    city_table = get_city_table(db_type)
    return (
        f"SELECT MAX({historical_table}.temperature) "
        f"FROM {historical_table} JOIN city ON {historical_table}.city_id = {city_table}.city_id "
        f"WHERE {city_table}.name = '{city}'"
    )


def get_current_temperature_query(city: str, db_type: str):
    current_table = get_current_table(db_type)
    city_table = get_city_table(db_type)
    return (
        f"SELECT {current_table}.temperature, {current_table}.time_measured FROM {current_table} "
        f"JOIN city ON {current_table}.city_id = {city_table}.city_id "
        f"WHERE city.name = '{city}';"
    )


def update_current_temperature_query(
    city_id: int, temperature: int, timestamp: str, db_type
):
    current_table = get_current_table(db_type)
    return (
        f"UPDATE {current_table} "
        f"SET "
        # f"city_id = {city_id}, "
        f"temperature = {temperature}, "
        f"time_measured = TIMESTAMP('{timestamp}') "
        f"WHERE city_id = {city_id}"
    )


def city_has_current_temperature_query(city_id: int, db_type: str):
    current_table = get_current_table(db_type)
    return f"SELECT city_id FROM {current_table} WHERE city_id = {city_id}"


def get_city_id_query(city: str, db_type: str):
    city_table = get_city_table(db_type)
    return f"SELECT city_id FROM {city_table} WHERE name = '{city}'"


def flush_table_query(table_name: str):
    return f"DELETE FROM {table_name}"


def create_table_query(table_name, db_type):
    table_info = get_table_info()
    column_data = table_info[table_name][db_type]
    s = f"CREATE TABLE {table_name}( "
    for col in column_data.keys():
        datatype = column_data[col]["type"]
        null = column_data[col]["null"]
        primary = column_data[col]["primary"]
        s += f"{col} {datatype} {null} {primary}, "
    s = s[:-2] + ")"
    return s


def drop_table_query(table_name):
    return f"DROP TABLE IF EXISTS {table_name}"


def get_max_entry(entry: str, table_name: str, city_id: int = None) -> str:
    if city_id is None:
        return f"SELECT MAX({entry}) from {table_name}"
    else:
        return f"SELECT MAX({entry}) from {table_name} WHERE city_id = {city_id}"
