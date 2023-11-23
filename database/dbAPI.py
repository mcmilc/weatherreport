import mysql.connector
from mysql.connector import errorcode
from southbayweather.config.config import sbw_root
from southbayweather.utilities.helpers import pjoin
from southbayweather.utilities.helpers import read_json
from southbayweather.utilities.helpers import generate_uuid
from southbayweather.utilities.helpers import round_val
from southbayweather.database.queries import generate_flush_table_query
from southbayweather.database.queries import get_city_id_query
from southbayweather.database.queries import add_historical_temperature
from southbayweather.database.queries import add_current_temperature


def MySQLConnectorFactory():
    passwd = read_json(pjoin(sbw_root, "data", "access.json"))["mysql"]["passwd"]
    database = read_json(pjoin(sbw_root, "data", "access.json"))["mysql"]["db_name"]
    return mysql.connector.connect(
        host="localhost", user="root", password=passwd, database=database
    )


def get_errorcode_flag(code):
    flag = [x for x in dir(errorcode) if getattr(errorcode, x) == code]
    if len(flag) == 1:
        return flag[0]


class MySQLAPI:
    def __init__(self, connector=MySQLConnectorFactory()):
        self.connector = connector

    def _get_city_id(self, city: str) -> str:
        cursor = self.connector.cursor()
        query = get_city_id_query(city)
        self._execute_query(cursor=cursor, query=query)
        city_id = None
        for result in cursor:
            city_id = result
        if city_id is None:
            raise Exception(f"No city named '{city}' in database")
        elif len(city_id) != 1:
            raise Exception(f"Multiple city ids found for '{city}'")
        city_id = str(city_id[0])
        cursor.close()
        return city_id

    def _execute_query(self, cursor, query, params=None):
        try:
            cursor.execute(query, params=params)
        except mysql.connector.Error as err:
            print(
                f"MYSQL ERROR: {err.msg}, ERROR-CODE-FLAG: {get_errorcode_flag(err.errno)}"
            )

    def populate_historical_temperature(
        self, timestamps: list, historical_temperature: list, city: int
    ) -> None:
        """Write historical temperature into database

        Args:
            historical_temperature (dict)
        """
        cursor = self.connector.cursor()
        city_id = self._get_city_id(city)
        for s_time, temp in zip(timestamps, historical_temperature):
            if temp is not None:
                uuid = generate_uuid(s_time=s_time)
                params = {
                    "historical_temperature_id": uuid,
                    "city_id": city_id,
                    "time_measured": s_time,
                    "temperature": round_val(temp),
                }
                self._execute_query(
                    cursor=cursor, query=add_historical_temperature, params=params
                )
        # commit required for DELETE
        self.connector.commit()
        cursor.close()

    def populate_current_temperature(
        self, timestamp: str, current_temperature: int, city: str
    ):
        cursor = self.connector.cursor()
        city_id = self._get_city_id(city)
        uuid = generate_uuid(s_time=timestamp)
        params = {
            "current_temperature_id": uuid,
            "city_id": city_id,
            "time_measured": timestamp,
            "temperature": current_temperature,
        }
        self._execute_query(cursor=cursor, query=add_current_temperature, params=params)
        self.connector.commit()
        cursor.close()

    def flush_table(self, table_name):
        cursor = self.connector.cursor()
        query = generate_flush_table_query(table_name)
        self._execute_query(cursor=cursor, query=query)
        # commit required for DELETE
        self.connector.commit()
        cursor.close()


if __name__ == "__main__":
    mydb = MySQLConnectorFactory()
    s_time = "2023-11-09T00:00"
    uuid = generate_uuid(s_time=s_time)
    historical_entry = {
        "historical_temperature_id": uuid,
        "city_id": 1,
        "time_measured": s_time,
        "temperature": 67,
    }
    cur = mydb.cursor()
    cur.execute(add_historical_temperature, historical_entry)
    mydb.commit()
    cur.close()
    mydb.close()
