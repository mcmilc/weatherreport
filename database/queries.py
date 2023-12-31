"""Wrappers around SQL queries."""
from weatherreport.data.helpers import get_table_info
from weatherreport.data.helpers import get_database_access_info
from weatherreport.data.helpers import get_table_name_historical_temperature
from weatherreport.data.helpers import get_table_name_current_temperature
from weatherreport.data.helpers import get_table_name_city_type
from weatherreport.data.helpers import get_table_name_city


def append_bigquery_prefix_to_table(db_type, table_name):
    if db_type == "bigquery":
        _access_info = get_database_access_info("bigqiery")
        project_id = _access_info["project_id"]
        dataset = _access_info["db_name"]
        return project_id + "." + dataset + "." + table_name
    else:
        return table_name


def add_historical_temperature_query(db_type):
    historical_table = get_table_name_historical_temperature(db_type)
    return (
        f"INSERT INTO {historical_table} (historical_temperature_id, city_id, time_measured, temperature) "
        f"VALUES (%(historical_temperature_id)s, %(city_id)s, TIMESTAMP('%(time_measured)s'), %(temperature)s)"
    )


def add_current_temperature_query(db_type):
    current_table = get_table_name_current_temperature(db_type)
    return (
        f"INSERT INTO {current_table} (city_id, time_measured, temperature) "
        f"VALUES (%(city_id)s, TIMESTAMP('%(time_measured)s'), %(temperature)s)"
    )


def add_city_type_query(db_type):
    city_type_table = get_table_name_city_type(db_type)
    return (
        f"INSERT INTO {city_type_table} (city_type_id, name) "
        f"VALUES (%(city_type_id)s, %(name)s)"
    )


def add_city_query(db_type):
    city_table = get_table_name_city(db_type)
    return (
        f"INSERT INTO {city_table} (city_id, name, city_type_id, longitude, latitude) "
        f"VALUES (%(city_id)s, %(name)s, %(city_type_id)s, %(longitude)s, %(latitude)s)"
    )


def get_all_max_historical_temperatures_query(db_type):
    historical_table = get_table_name_historical_temperature(db_type)
    city_table = get_table_name_city(db_type)
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
    historical_table = get_table_name_historical_temperature(db_type)
    return (
        f"SELECT time_measured "
        f"FROM {historical_table} "
        f"JOIN city ON {historical_table}.city_id = city.city_id "
        f"WHERE city.name = '{city}' AND {historical_table}.temperature = {temperature}"
    )


def get_max_historical_temperature_for_city_query(city: int, db_type: str):
    historical_table = get_table_name_historical_temperature(db_type)
    city_table = get_table_name_city(db_type)
    return (
        f"SELECT MAX({historical_table}.temperature) "
        f"FROM {historical_table} JOIN city ON {historical_table}.city_id = {city_table}.city_id "
        f"WHERE {city_table}.name = '{city}'"
    )


def get_current_temperature_query(city: str, db_type: str):
    current_table = get_table_name_current_temperature(db_type)
    city_table = get_table_name_city(db_type)
    return (
        f"SELECT {current_table}.temperature, {current_table}.time_measured FROM {current_table} "
        f"JOIN city ON {current_table}.city_id = {city_table}.city_id "
        f"WHERE city.name = '{city}';"
    )


def update_current_temperature_query(
    city_id: int, temperature: int, timestamp: str, db_type
):
    current_table = get_table_name_current_temperature(db_type)
    return (
        f"UPDATE {current_table} "
        f"SET "
        # f"city_id = {city_id}, "
        f"temperature = {temperature}, "
        f"time_measured = TIMESTAMP('{timestamp}') "
        f"WHERE city_id = {city_id}"
    )


def city_has_current_temperature_query(city_id: int, db_type: str):
    current_table = get_table_name_current_temperature(db_type)
    return f"SELECT city_id FROM {current_table} WHERE city_id = {city_id}"


def get_city_id_query(city: str, db_type: str):
    city_table = get_table_name_city(db_type)
    return f"SELECT city_id FROM {city_table} WHERE name = '{city}'"


def flush_table_query(table_name: str):
    return f"DELETE FROM {table_name} WHERE 1=1"


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


def get_max_historical_temperature_id(db_type) -> str:
    """Get max PK entry of historical temperature table."""
    historical_table = get_table_name_historical_temperature(db_type)
    return f"SELECT MAX(historical_temperature_id) FROM {historical_table}"


def get_max_temperature_delta_between_city_pair(city_id_1, city_id_2):
    return (
        f"SELECT MAX(ABS(tab_1.temp_1 - tab_2.temp_2)) "
        f"FROM (SELECT city_id as city_id_1, time_measured as time_1, temperature as temp_1 "
        f"FROM historical_temperature WHERE city_id = {city_id_1} ) tab_1 "
        f"JOIN (SELECT city_id as city_id_2, time_measured as time_2, "
        f"temperature as temp_2 FROM historical_temperature WHERE city_id = {city_id_2}) tab_2 "
        f"ON tab_1.time_1 = tab_2.time_2"
    )


def remove_entries_with_duplicated_timestamps(db_type, city_id):
    historical_table = get_table_name_historical_temperature(db_type)
    return (
        f"DELETE "
        f"FROM "
        f"{historical_table} "
        f"WHERE "
        f"historical_temperature_id IN ( "
        f"SELECT "
        f"table_2.historical_temperature_id "
        f"FROM ( "
        f"SELECT "
        f"historical_temperature_id, "
        f"city_id, "
        f"time_measured, "
        f"temperature, "
        f"ROW_NUMBER() OVER(PARTITION BY time_measured ORDER BY historical_temperature_id DESC) AS Row_Number "
        f"FROM ( "
        f"SELECT "
        f"* "
        f"FROM "
        f"{historical_table} "
        f"WHERE "
        f"city_id = {city_id}) table_1 ) table_2 "
        f"WHERE "
        f"table_2.Row_Number > 1) "
    )


"""
SELECT
tab_1.time_1 AS DELTA_TIME,
ABS(tab_1.temp_1 - tab_2.temp_2) AS DELTA
FROM 
	(
	SELECT
    	city_id as city_id_1,
		time_measured as time_1,
    	temperature as temp_1
	FROM `historical_temperature`
	WHERE city_id = 4) tab_1
JOIN
(	
    SELECT
 		city_id as city_id_2,
 		time_measured as time_2,
 		temperature as temp_2
		FROM `historical_temperature` 
		WHERE city_id = 8) tab_2 
ON tab_1.time_1 = tab_2.time_2
ORDER BY DELTA DESC
LIMIT 1;
"""
