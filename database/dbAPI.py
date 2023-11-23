import mysql.connector
from mysql.connector import errorcode
from southbayweather.config.config import sbw_root
from southbayweather.utilities.helpers import pjoin
from southbayweather.utilities.helpers import read_json
from southbayweather.utilities.helpers import generate_uuid
from southbayweather.utilities.helpers import round_val
from southbayweather.database.queries import generate_flush_table_query
from southbayweather.database.queries import add_historical_temperature


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
        self.con = connector

    def populate_historical_temperature(
        self, timestamps: list, historical_temperature: list, city_id: int
    ) -> None:
        """Write historical temperature into database

        Args:
            historical_temperature (dict)
        """
        cur = self.con.cursor()
        for s_time, temp in zip(timestamps, historical_temperature):
            if temp is not None:
                uuid = generate_uuid(s_time=s_time)
                historical_entry = {
                    "historical_temperature_id": uuid,
                    "city_id": city_id,
                    "time_measured": s_time,
                    "temperature": round_val(temp),
                }
                try:
                    cur.execute(add_historical_temperature, historical_entry)
                except mysql.connector.Error as err:
                    print(
                        f"MYSQL ERROR: {err.msg}, ERROR-CODE-FLAG: {get_errorcode_flag(err.errno)}"
                    )
                    continue

        self.con.commit()
        cur.close()

    def flush_table(self, table_name):
        cur = self.con.cursor()
        query = generate_flush_table_query(table_name)
        cur.execute(query)
        self.con.commit()
        cur.close()


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
