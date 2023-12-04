"""First version of two APIs supporting MySQL and BigQuery. This is a first draft. Need to refactor using better OOP and / or context managers.
"""
import mysql.connector
import pandas as pd
from google.cloud import bigquery
from mysql.connector import errorcode

# ACCESS

# HELPERS
from weatherreport.utilities.helpers import setup_bigquery_environment
from weatherreport.utilities.helpers import get_connection_passwd
from weatherreport.utilities.helpers import get_connection_database
from weatherreport.utilities.helpers import generate_uuid
from weatherreport.utilities.helpers import round_val
from weatherreport.utilities.helpers import file_exists
from weatherreport.utilities.helpers import get_city_id
from weatherreport.utilities.helpers import get_city_info
from weatherreport.utilities.helpers import get_table_info
from weatherreport.utilities.helpers import get_city_type_info
from weatherreport.utilities.helpers import get_access_info

# QUERIES
from weatherreport.database.queries import flush_table_query
from weatherreport.database.queries import get_city_id_query
from weatherreport.database.queries import add_historical_temperature_query
from weatherreport.database.queries import add_current_temperature_query
from weatherreport.database.queries import add_city_type_query
from weatherreport.database.queries import add_city_query
from weatherreport.database.queries import update_current_temperature_query
from weatherreport.database.queries import city_has_current_temperature_query
from weatherreport.database.queries import (
    get_max_historical_temperature_for_city_query,
)
from weatherreport.database.queries import (
    get_max_historical_temperature_timestamps_query,
)
from weatherreport.database.queries import get_all_max_historical_temperatures_query
from weatherreport.database.queries import get_current_temperature_query
from weatherreport.database.queries import create_table_query
from weatherreport.database.queries import drop_table_query
from weatherreport.database.queries import get_historical_table


def unwrap_sql_result(func):
    def wrapper(self, query_string, params, commit=False):
        wrapped_result = func(self, query_string, params, commit=False)
        result = None
        try:
            result = wrapped_result[0][0]
        except IndexError:
            pass
        return result

    return wrapper


def mysql_query(func):
    def wrapper(self, query_string, params, commit=False):
        cursor = self.client.cursor()
        func(self, cursor, query_string=query_string, params=params, commit=commit)
        results = []
        try:
            for r in cursor:
                results.append(r)
        except mysql.connector.errors.InterfaceError:
            pass
        cursor.close()
        return results

    return wrapper


def DBAPIFactory(db_type="mysql"):
    if db_type == "mysql":
        passwd = get_connection_passwd(db_type=db_type)
        database = get_connection_passwd(db_type=db_type)
        return MySQLAPI(client=MySQLClient(db_type=db_type))
    elif db_type == "cloud-sql":
        passwd = get_connection_passwd(db_type=db_type)
        database = get_connection_passwd(db_type=db_type)
        cnx = mysql.connector.connect(
            host="localhost",
            user="root",
            password=passwd,
            database=database,
            instance_connections_str="bubbly-mission-402701:us-central1:db-mysql-mcmil-01",
            driver="pymysql",
        )
        return MySQLAPI(cnx)
    elif db_type == "bigquery":
        access_info = get_access_info(db_type)
        return BigQueryAPI(
            BigQueryClient(
                project_id=access_info["project_id"], dataset=access_info["db_name"]
            )
        )
    elif db_type == "csv":
        return CSVAPI()


def convert_timestamp(timestamp):
    return str.replace(timestamp, "T", " ") + ":00"


def get_errorcode_flag(code):
    flag = [x for x in dir(errorcode) if getattr(errorcode, x) == code]
    if len(flag) == 1:
        return flag[0]


def execute_query(cursor, query, params):
    pass


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


class DBClient:
    def __init__(self):
        pass

    def execute_query(self, *args, **kwargs):
        pass


class DBAPI:
    def __init__(self, client: DBClient, *args, **kwargs):
        self.client = client

    def get_city_id(self, city: str) -> str:
        return get_city_id(city)

    def execute_query(self, query_string: str, params: str = None, **kwargs):
        return self.client.execute_query(query_string, params, **kwargs)


class BigQueryClient(DBClient):
    def __init__(self, project_id: str, dataset: str):
        setup_bigquery_environment("weather-report-406515-20573f2148d1.json")
        self.project_id = project_id
        self.dataset = dataset
        self.client = bigquery.Client(project=project_id)
        self.default_job_config = bigquery.QueryJobConfig(
            default_dataset=project_id + "." + dataset
        )

    def execute_query(self, query_string: str, params: dict = None):
        if params is not None:
            query_string = query_string % params
        # print(query_string)
        job = self.client.query(query=query_string, job_config=self.default_job_config)
        results = []
        for out in job.result():
            results.append(out)
        return results


class MySQLClient(DBClient):
    def __init__(self, db_type, host="localhost", user="root"):
        passwd = get_connection_passwd(db_type)
        database = get_connection_database(db_type)
        self.client = mysql.connector.connect(
            host=host, user=user, password=passwd, database=database
        )

    @mysql_query
    def execute_query(
        self, cursor, query_string: str, params: str = None, commit: bool = False
    ):
        try:
            cursor.execute(query_string, params=params)
        except mysql.connector.Error as err:
            print(
                f"MYSQL ERROR: {err.msg}, ERROR-CODE-FLAG: {get_errorcode_flag(err.errno)}"
            )
        if commit:
            self.client.commit()


class CSVAPI:
    def populate_historical_temperature(
        self,
        timestamps: list,
        temperatures: list,
        city: str,
        filename: str,
    ):
        table_info = get_table_info()
        city_id = get_city_id(city)
        data = []
        columns = table_info[get_historical_table]["bigquery"].keys()
        for s_time, temperature in zip(timestamps, temperatures):
            if temperature is not None:
                uuid = generate_uuid(s_time=s_time, city_id=city_id)
                time_measured = convert_timestamp(s_time)
                temperature = round_val(temperature)
                data.append((uuid, city_id, time_measured, temperature))
        df = pd.DataFrame(columns=columns, data=data)
        if file_exists(filename):
            _df = pd.read_csv(filename)
            df = pd.concat([_df, df])
        df.to_csv(filename, columns=columns, index=False)


class BigQueryAPI(DBAPI):
    def __init__(
        self,
        client: BigQueryClient,
    ):
        self.client = client

    def _extend_table_name(self, table_name):
        return f"`{self.client.project_id}.{self.client.dataset}.{table_name}`"

    def drop_table(self, table_name):
        table_name = self._extend_table_name(table_name)
        self.execute_query(
            query_string=drop_table_query(table_name=table_name), params=None
        )

    def create_table(self, table_name):
        query = create_table_query(table_name=table_name, db_type="bigquery")
        self.execute_query(query_string=query)

    def city_has_current_temperature(self, city: str) -> bool:
        city_id = self.get_city_id(city)
        return self.execute_query(
            query_string=city_has_current_temperature_query(
                city_id=city_id, db_type="bigquery"
            )
        )

    def populate_historical_temperature(
        self, timestamps: list, temperatures: list, city: str
    ) -> None:
        """Write historical temperature into database. Very slow.

        Args:
            timestamps (list datetime.datetime)
            temperatures (list float)
            city (str)
        """
        city_id = self.get_city_id(city)
        for s_time, temperature in zip(timestamps, temperatures):
            if temperature is not None:
                uuid = generate_uuid(s_time=s_time, city_id=city_id)
                time_measured = convert_timestamp(s_time)
                temperature = round_val(temperature)
                params = {
                    "historical_temperature_id": uuid,
                    "city_id": city_id,
                    "time_measured": time_measured,
                    "temperature": temperature,
                }
                self.execute_query(
                    query_string=add_historical_temperature_query("bigquery"),
                    params=params,
                )

    def populate_current_temperature(
        self, timestamp: str, temperature: float, city: str
    ):
        city_id = self.get_city_id(city)
        time_measured = convert_timestamp(timestamp)
        if self.city_has_current_temperature(city):
            self.execute_query(
                query_string=update_current_temperature_query(
                    city_id=city_id,
                    temperature=round_val(temperature),
                    timestamp=time_measured,
                    db_type="bigquery",
                )
            )
        else:
            params = {
                "city_id": city_id,
                "time_measured": time_measured,
                "temperature": round_val(temperature),
            }
            self.execute_query(
                query_string=add_current_temperature_query(db_type="bigquery"),
                params=params,
            )

    def populate_city_type(self):
        city_type_info = get_city_type_info()
        for city_type in city_type_info.keys():
            params = {"city_type_id": city_type_info[city_type], "name": city_type}
            self.execute_query(
                query_string=add_city_type_query(db_type="bigquery"), params=params
            )

    def populate_city(self):
        city_info = get_city_info()
        for city in city_info.keys():
            params = {
                "city_id": city_info[city]["city_id"],
                "name": city,
                "city_type_id": city_info[city]["city_type_id"],
                "longitude": city_info[city]["longitude"],
                "latitude": city_info[city]["latitude"],
            }
            self.execute_query(
                query_string=add_city_query(db_type="bigquery"), params=params
            )

    def upload_csv_data(self, table_name, filename):
        table_info = get_table_info()
        column_data = table_info[table_name]["bigquery"]
        schema = []
        for col in column_data.keys():
            if column_data[col]["null"] == "NOT NULL":
                mode = "REQUIRED"
            else:
                mode = "NULLABLE"
            schema.append(
                bigquery.SchemaField(
                    name=col,
                    field_type=column_data[col]["type"],
                    mode=mode,
                )
            )
        jc = bigquery.job.LoadJobConfig(
            schema=schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )
        self.client.client.load_table_from_file(
            open(filename, "rb"),
            destination=self.client.project_id
            + "."
            + self.client.dataset
            + "."
            + table_name,
            job_config=jc,
        )


class MySQLAPI(DBAPI):
    def __init__(self, client):
        self.client = client

    def drop_table(self, table_name):
        self.execute_query(query_string=drop_table_query(table_name), commit=True)

    def create_table(self, table_name):
        self.execute_query(
            query_string=create_table_query(table_name=table_name, db_type="mysql"),
            commit=True,
        )

    def get_city_id(self, city: str) -> str:
        return self.execute_query(
            query_string=get_city_id_query(city=city, db_type="mysql")
        )[0][0]

    def city_has_current_temperature(self, city: str) -> bool:
        city_id = self.get_city_id(city)
        return self.execute_query(
            query_string=city_has_current_temperature_query(
                city_id=city_id, db_type="mysql"
            )
        )[0][0]

    def populate_historical_temperature(
        self, timestamps: list, temperature: list, city: str
    ) -> None:
        """Write historical temperature into database

        Args:
            timestamps (list datetime.datetime)
            temperature (dict)
            city (str)
        """
        city_id = self.get_city_id(city)
        for s_time, temp in zip(timestamps, temperature):
            if temp is not None:
                uuid = generate_uuid(s_time=s_time, city_id=city_id)
                params = {
                    "historical_temperature_id": uuid,
                    "city_id": city_id,
                    "time_measured": str.replace(s_time, "T", " ") + ":00",
                    "temperature": round_val(temp),
                }
                self.execute_query(
                    query_string=add_historical_temperature_query("mysql"),
                    params=params,
                    commit=True,
                )

    def populate_current_temperature(self, timestamp: str, temperature: int, city: str):
        city_id = self.get_city_id(city)
        if self.city_has_current_temperature(city):
            self.execute_query(
                query_string=update_current_temperature_query(
                    city_id=city_id,
                    temperature=round_val(temperature),
                    timestamp=timestamp,
                    db_type="mysql",
                ),
                params=None,
                commit=True,
            )
        else:
            params = {
                "city_id": city_id,
                "time_measured": timestamp,
                "temperature": round_val(temperature),
            }
            self.execute_query(
                query_string=add_current_temperature_query("mysql"),
                params=params,
                commit=True,
            )

    def get_max_temperature(self, city: str):
        return self.execute_query(
            query_string=get_max_historical_temperature_for_city_query(
                city=city, db_type="mysql"
            )
        )[0][0]

    def get_max_temperature_timestamps(self, city, temperature):
        query = get_max_historical_temperature_timestamps_query(
            city=city, temperature=temperature, db_type="mysql"
        )
        return self.execute_query(query_string=query)[0]

    def get_all_max_temperatures(self):
        query = get_all_max_historical_temperatures_query(db_type="mysql")
        return self.execute_query(query_string=query)

    def get_current_temperature(self, city: str):
        if self.city_has_current_temperature(city):
            query = get_current_temperature_query(city=city, db_type="mysql")
            return execute_query(query_string=query)[0][0]

    def flush_table(self, table_name):
        query = flush_table_query(table_name=table_name)
        self.execute_query(query_string=query, commit=True)


if __name__ == "__main__":
    pass
