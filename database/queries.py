from weatherreport.config.config import sbw_root
from weatherreport.utilities.helpers import pjoin
from weatherreport.utilities.helpers import read_json
from weatherreport.utilities.helpers import get_table_info

access_info = pjoin(sbw_root, "data", "access.json")

historical_table = read_json(access_info)["mysql"]["tables"]["historical"]
current_table = read_json(access_info)["mysql"]["tables"]["current"]
city_table = read_json(access_info)["mysql"]["tables"]["city"]
city_type_table = read_json(access_info)["mysql"]["tables"]["city_type"]

add_historical_temperature_query = (
    f"INSERT INTO {historical_table} (historical_temperature_id, city_id, time_measured, temperature) "
    f"VALUES (%(historical_temperature_id)s, %(city_id)s, TIMESTAMP(%(time_measured)s), %(temperature)s)"
)

add_current_temperature_query = (
    f"INSERT INTO {current_table} (city_id, time_measured, temperature) "
    f"VALUES (%(city_id)s, %(time_measured)s, %(temperature)s)"
)

add_city_type_query = (
    f"INSERT INTO {city_type_table} (city_type_id, name) "
    f"VALUES (%(city_type_id)s, '%(name)s')"
)
add_city_query = (
    f"INSERT INTO {city_table} (city_id, name, city_type_id, longitude, latitude) "
    f"VALUES (%(city_id)s, '%(name)s', %(city_type_id)s, %(longitude)s, %(latitude)s)"
)


def get_all_city_names():
    city_info = pjoin(sbw_root, "data", "city_info.json")
    return read_json(city_info).keys()


def get_all_max_historical_temperatures_query():
    return (
        f"SELECT "
        f"  MAX({historical_table}.temperature) AS MAX_temperature, "
        f"  {city_table}.name "
        f"FROM {historical_table} "
        f"JOIN city ON {historical_table}.city_id = {city_table}.city_id "
        f"GROUP BY {city_table}.name;"
    )


def get_max_historical_temperature_timestamps_query(city: str, temperature: int):
    return (
        f"SELECT time_measured "
        f"FROM {historical_table} "
        f"JOIN city ON historical_temperature.city_id = city.city_id "
        f"WHERE city.name = '{city}' AND {historical_table}.temperature = {temperature}"
    )


def get_max_historical_temperature_for_city_query(city: int):
    return (
        f"SELECT MAX({historical_table}.temperature) "
        f"FROM {historical_table} JOIN city ON historical_temperature.city_id = {city_table}.city_id "
        f"WHERE {city_table}.name = '{city}'"
    )


def get_current_temperature_query(city: str):
    return (
        f"SELECT {current_table}.temperature, {current_table}.time_measured FROM current_temperature "
        f"JOIN city ON {current_table}.city_id = {city_table}.city_id "
        f"WHERE city.name = '{city}';"
    )


def update_current_temperature_query(city_id: int, temperature: int, timestamp: str):
    return (
        f"UPDATE {current_table} "
        f"SET "
        f"city_id = {city_id}, "
        f"temperature = {temperature}, "
        f"time_measured = '{timestamp}' "
        f"WHERE city_id = {city_id}"
    )


def has_city_current_temperature_query(city_id: int):
    return f"SELECT city_id FROM current_temperature WHERE city_id = {city_id}"


def get_city_id_query(city: str):
    return f"SELECT city_id FROM city WHERE name = '{city}'"


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
