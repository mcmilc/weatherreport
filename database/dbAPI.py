"""First version of two APIs supporting MySQL and BigQuery. This is a first draft. Need to refactor using better OOP and / or context managers.
"""
import os
import mysql.connector
from google.cloud import bigquery
from mysql.connector import errorcode
from southbayweather.config.config import sbw_root

# HELPERS
from southbayweather.utilities.helpers import pjoin
from southbayweather.utilities.helpers import read_json
from southbayweather.utilities.helpers import generate_uuid
from southbayweather.utilities.helpers import round_val

# QUERIES
from southbayweather.database.queries import flush_table_query
from southbayweather.database.queries import get_city_id_query
from southbayweather.database.queries import add_historical_temperature
from southbayweather.database.queries import add_current_temperature
from southbayweather.database.queries import update_current_temperature
from southbayweather.database.queries import has_city_current_temperature_query
from southbayweather.database.queries import get_max_historical_temperature_for_city
from southbayweather.database.queries import get_max_historical_temperature_timestamps
from southbayweather.database.queries import get_all_max_historical_temperatures
from southbayweather.database.queries import get_current_temperature


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = pjoin(
    sbw_root,
    "database",
    "bubbly-mission-402701-357b024147cf.json",
)


def MySQLAPIFactory():
    passwd = read_json(pjoin(sbw_root, "data", "access.json"))["mysql"]["passwd"]
    database = read_json(pjoin(sbw_root, "data", "access.json"))["mysql"]["db_name"]
    cnx = mysql.connector.connect(
        host="localhost", user="root", password=passwd, database=database
    )
    return MySQLAPI(cnx)


def get_errorcode_flag(code):
    flag = [x for x in dir(errorcode) if getattr(errorcode, x) == code]
    if len(flag) == 1:
        return flag[0]


def query_has_result(cursor) -> bool:
    output = []
    for result in cursor:
        output = result
    if len(output) == 0:
        return False
    else:
        return True


def extract_city_id(cursor, city) -> str:
    city_id = None
    for result in cursor:
        city_id = result
    if city_id is None:
        raise Exception(f"No city named '{city}' in database")
    elif len(city_id) != 1:
        raise Exception(f"Multiple city ids found for '{city}'")
    return str(city_id[0])


def execute_query(cursor, query, params=None):
    try:
        cursor.execute(query, params=params)
    except mysql.connector.Error as err:
        print(
            f"MYSQL ERROR: {err.msg}, ERROR-CODE-FLAG: {get_errorcode_flag(err.errno)}"
        )


class BigQueryAPI:
    def __init__(
        self,
        client=bigquery.Client(project="bubbly-mission-402701"),
    ):
        self.client = client
        self.jc = bigquery.QueryJobConfig(
            default_dataset="bubbly-mission-402701.southbay_weather_db"
        )

    def _get_city_id(self, city: str) -> str:
        job = self.client.query(get_city_id_query(city), job_config=self.jc)
        for row in job.result():
            city_id = row[0]
        return city_id

    def _has_city_current_temperature(self, city: str) -> bool:
        city_id = self._get_city_id(city)
        job = self.client.query(
            has_city_current_temperature_query(city_id), job_config=self.jc
        )
        result = query_has_result(job)
        return result

    def populate_historical_temperature(
        self, timestamps: list, temperature: list, city: int
    ) -> None:
        """Write historical temperature into database

        Args:
            temperature (dict)
        """
        city_id = self._get_city_id(city)
        for s_time, temp in zip(timestamps, temperature):
            if temp is not None:
                uuid = generate_uuid(s_time=s_time, city_id=city_id)
                params = {
                    "historical_temperature_id": uuid,
                    "city_id": city_id,
                    "time_measured": s_time,
                    "temperature": round_val(temp),
                }
                print("Query")
                print(add_historical_temperature % params)
                job = self.client.query(
                    add_historical_temperature % params, job_config=self.jc
                )
                job.result()
                print(f"added {params}")


class MySQLAPI:
    def __init__(self, connector):
        self.connector = connector

    def _get_city_id(self, city: str) -> str:
        cursor = self.connector.cursor()
        query = get_city_id_query(city)
        execute_query(cursor=cursor, query=query)
        city_id = extract_city_id(cursor, city)
        cursor.close()
        return city_id

    def _has_city_current_temperature(self, city: str) -> bool:
        cursor = self.connector.cursor()
        city_id = self._get_city_id(city)
        query = has_city_current_temperature_query(city_id)
        execute_query(cursor=cursor, query=query)
        result = query_has_result(cursor)
        cursor.close()
        return result

    def populate_historical_temperature(
        self, timestamps: list, temperature: list, city: int
    ) -> None:
        """Write historical temperature into database

        Args:
            temperature (dict)
        """
        cursor = self.connector.cursor()
        city_id = self._get_city_id(city)
        for s_time, temp in zip(timestamps, temperature):
            if temp is not None:
                uuid = generate_uuid(s_time=s_time, city_id=city_id)
                params = {
                    "historical_temperature_id": uuid,
                    "city_id": city_id,
                    "time_measured": s_time,
                    "temperature": round_val(temp),
                }
                execute_query(
                    cursor=cursor, query=add_historical_temperature, params=params
                )
        # commit required for DELETE
        self.connector.commit()
        cursor.close()

    def populate_current_temperature(self, timestamp: str, temperature: int, city: str):
        cursor = self.connector.cursor()
        city_id = self._get_city_id(city)
        result = self._has_city_current_temperature(city)
        if result:
            execute_query(
                cursor=cursor,
                query=update_current_temperature(
                    city_id, round_val(temperature), timestamp
                ),
            )
        else:
            params = {
                "city_id": city_id,
                "time_measured": timestamp,
                "temperature": round_val(temperature),
            }
            execute_query(cursor=cursor, query=add_current_temperature, params=params)
        self.connector.commit()
        cursor.close()

    def get_max_temperature(self, city: str):
        cursor = self.connector.cursor()
        query = get_max_historical_temperature_for_city(city)
        execute_query(cursor=cursor, query=query)
        max_temperature = None
        for result in cursor:
            max_temperature = result[0]
        cursor.close()
        return max_temperature

    def get_max_temperature_timestamps(self, city, temperature):
        cursor = self.connector.cursor()
        query = get_max_historical_temperature_timestamps(
            city=city, temperature=temperature
        )
        execute_query(cursor=cursor, query=query)
        timestamps = []
        for result in cursor:
            timestamps.append(result[0])
        cursor.close()
        return timestamps

    def get_all_max_temperatures(self):
        cursor = self.connector.cursor()
        query = get_all_max_historical_temperatures()
        execute_query(cursor=cursor, query=query)
        output = []
        for result in cursor:
            output.append(result)
        cursor.close()
        return output

    def get_current_temperature(self, city: str):
        if self._has_city_current_temperature(city):
            cursor = self.connector.cursor()
            query = get_current_temperature(city)
            execute_query(cursor=cursor, query=query)
            temperature = None
            for result in cursor:
                temperature, timestamp = result
            cursor.close()
            return temperature, timestamp

    def flush_table(self, table_name):
        cursor = self.connector.cursor()
        query = flush_table_query(table_name)
        execute_query(cursor=cursor, query=query)
        # commit required for DELETE
        self.connector.commit()
        cursor.close()


if __name__ == "__main__":
    pass
