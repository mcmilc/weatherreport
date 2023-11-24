from southbayweather.config.config import sbw_root
from southbayweather.utilities.helpers import pjoin
from southbayweather.utilities.helpers import read_json

access_info = pjoin(sbw_root, "data", "access.json")

historical_table = read_json(access_info)["mysql"]["tables"]["historical"]
current_table = read_json(access_info)["mysql"]["tables"]["current"]

add_historical_temperature = (
    f"INSERT INTO {historical_table} (historical_temperature_id, city_id, time_measured, temperature)"
    f"Values (%(historical_temperature_id)s, %(city_id)s, %(time_measured)s, %(temperature)s)"
)

add_current_temperature = (
    f"INSERT INTO {current_table} (city_id, time_measured, temperature)"
    f"Values (%(city_id)s, %(time_measured)s, %(temperature)s)"
)


def update_current_temperature(city_id: int, temperature: int, timestamp: str):
    update_current_temperature = (
        f"UPDATE {current_table} "
        f"SET "
        f"city_id = {city_id}, "
        f"temperature = {temperature}, "
        f"time_measured = '{timestamp}' "
        f"WHERE city_id = {city_id}"
    )
    return update_current_temperature


def has_city_current_temperature_query(city_id: int):
    return f"SELECT city_id FROM current_temperature WHERE city_id = {city_id}"


def get_city_id_query(city: str):
    return f"SELECT city_id FROM city WHERE name = '{city}'"


def flush_table_query(table_name: str):
    return f"DELETE FROM {table_name}"
