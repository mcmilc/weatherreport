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
    f"INSERT INTO {current_table} (current_temperature_id, city_id, time_measured, temperature)"
    f"Values (%(current_temperature_id)s, %(city_id)s, %(time_measured)s, %(temperature)s)"
)


def get_city_id_query(city):
    return f"SELECT city_id FROM city WHERE name = '{city}'"


def generate_flush_table_query(table_name):
    return f"DELETE FROM {table_name}"
