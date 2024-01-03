"""Python level wrappers around various databse connectors.
"""
from abc import abstractmethod
from abc import ABC
import datetime as dt
import mysql.connector
import pandas as pd
from google.cloud import bigquery

# ACCESS

# HELPERS
from weatherreport.utilities.helpers import setup_bigquery_environment
from weatherreport.utilities.filesystem_utils import pexists
from weatherreport.utilities.helpers import get_errorcode_flag

from weatherreport.data.helpers import get_connection_passwd_info
from weatherreport.data.helpers import get_database_name_info
from weatherreport.data.helpers import get_city_id_from_info
from weatherreport.data.helpers import get_city_info
from weatherreport.data.helpers import get_table_info
from weatherreport.data.helpers import get_city_type_info
from weatherreport.data.helpers import get_database_access_info
from weatherreport.data.helpers import get_table_name_historical_temperature

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
from weatherreport.database.queries import get_max_entry
from weatherreport.database.queries import get_max_historical_temperature_id
from weatherreport.database.queries import remove_entries_with_duplicated_timestamps


def db_wrapper_factory(db_type):
    """Factory of database apis."""
    if db_type == "mysql":
        passwd = get_connection_passwd_info(db_type=db_type)
        database = get_connection_passwd_info(db_type=db_type)
        return MySQLWrapper(client=MySQLClient(db_type=db_type))
    elif db_type == "cloud-sql":
        passwd = get_connection_passwd_info(db_type=db_type)
        database = get_connection_passwd_info(db_type=db_type)
        cnx = mysql.connector.connect(
            host="localhost",
            user="root",
            password=passwd,
            database=database,
            instance_connections_str="bubbly-mission-402701:us-central1:db-mysql-mcmil-01",
            driver="pymysql",
        )
        return MySQLWrapper(cnx)
    elif db_type == "bigquery":
        access_info = get_database_access_info(db_type)
        return BigQueryWrapper(
            BigQueryClient(
                project_id=access_info["project_id"], dataset=access_info["db_name"]
            )
        )
    elif db_type == "csv":
        return CSVWrapper()


class DBClient(ABC):
    """Baseclass representing a database client / connector."""

    @abstractmethod
    def execute_query(self, query_string, params, commit):
        """Needs to be implemented by subclass."""
        raise NotImplementedError


class DBWrapper:
    """Baseclass for database APIs"""

    def __init__(self, client: DBClient):
        self.client = client
        self._db_type = None

    def city_has_current_temperature(self, city: str) -> bool:
        """Returns True if city has current temperature entry in DB otherwise False.

        Args:
            city (str):

        Returns:
            bool:
        """
        city_id = get_city_id_from_info(city)
        result = self.client.execute_query(
            query_string=city_has_current_temperature_query(
                city_id=city_id, db_type=self._db_type
            )
        )
        if len(result) > 0:
            return True
        return False

    def get_city_id(self, city: str) -> str:
        """Return city_id for city.

        Args:
            city (str):

        Returns:
            str:
        """
        result = self.client.execute_query(
            query_string=get_city_id_query(city=city, db_type=self._db_type)
        )
        if len(result) > 0:
            return result[0][0]
        return None

    def drop_table(self, table_name):
        """Removes table table_name from database."""
        query = drop_table_query(table_name)
        self.client.execute_query(query_string=query, commit=True)

    def create_table(self, table_name):
        """Creates table table_name in database."""
        query = create_table_query(table_name=table_name, db_type=self._db_type)
        self.client.execute_query(
            query_string=query,
            commit=True,
        )

    def load_historical_temperature(
        self, timestamps: list, temperatures: list, city: str
    ) -> None:
        """Upload historical temperature to database

        Args:
            timestamps (list datetime.datetime)
            temperature (dict)
            city (str)
        """
        uuid = self.get_max_historical_temperature_id() + 1
        max_timestamp = None
        if uuid > 1:
            max_timestamp = self.get_latest_historical_temperature_timestamp(city=city)
        city_id = get_city_id_from_info(city)
        for s_time, temp in zip(timestamps, temperatures):
            if temp is not None:
                # this is incorrect in terms of normalization
                # uuid = generate_uuid(s_time=s_time, city_id=city_id)
                # current_timestamp = convert_timestamp(s_time)
                if max_timestamp is not None:
                    if max_timestamp > dt.datetime.fromisoformat(s_time):
                        # print(
                        #    f"Disgarding timestamp {s_time} since current latest entry is newer."
                        # )
                        continue
                params = {
                    "historical_temperature_id": uuid,
                    "city_id": city_id,
                    "time_measured": s_time,
                    "temperature": temp,
                }
                query = add_historical_temperature_query(self._db_type)
                self.client.execute_query(
                    query_string=query,
                    params=params,
                    commit=True,
                )
                uuid += 1

    def load_current_temperature(self, timestamp: str, temperature: int, city: str):
        """Upload current temperature data to database.

        Args:
            timestamp (str): _description_
            temperature (int): _description_
            city (str): _description_
        """
        city_id = get_city_id_from_info(city)
        if self.city_has_current_temperature(city):
            query = update_current_temperature_query(
                city_id=city_id,
                temperature=temperature,
                timestamp=timestamp,
                db_type=self._db_type,
            )
            self.client.execute_query(
                query_string=query,
                params=None,
                commit=True,
            )
        else:
            params = {
                "city_id": city_id,
                "time_measured": timestamp,
                "temperature": temperature,
            }
            query = add_current_temperature_query(self._db_type)
            query = query % params
            self.client.execute_query(
                query_string=query,
                params=None,
                commit=True,
            )

    def load_city_type(self):
        """Upload city_type data to database."""
        city_type_info = get_city_type_info()
        for city_type in city_type_info.keys():
            params = {"city_type_id": city_type_info[city_type], "name": city_type}
            self.client.execute_query(
                query_string=add_city_type_query(db_type=self._db_type),
                params=params,
                commit=True,
            )

    def load_city(self):
        """Upload city data to database."""
        city_info = get_city_info()
        for city in city_info.keys():
            params = {
                "city_id": city_info[city]["city_id"],
                "name": city,
                "city_type_id": city_info[city]["city_type_id"],
                "longitude": city_info[city]["longitude"],
                "latitude": city_info[city]["latitude"],
            }
            query = add_city_query(db_type=self._db_type)
            self.client.execute_query(
                query_string=query,
                params=params,
                commit=True,
            )

    def get_latest_historical_temperature_timestamp(self, city: str):
        """Get the timestamp of the most recent historical temperature
        entry for city."""
        city_id = get_city_id_from_info(city)
        result = self.client.execute_query(
            get_max_entry(
                entry="time_measured",
                table_name=get_table_name_historical_temperature(self._db_type),
                city_id=city_id,
            )
        )
        if len(result) > 0:
            if result[0][0] is not None:
                return result[0][0]
        return None

    def get_max_temperature(self, city: str):
        """Get maximal historical temperature for city.

        Args:
            city (str): _description_
        """
        result = self.client.execute_query(
            query_string=get_max_historical_temperature_for_city_query(
                city=city, db_type=self._db_type
            )
        )
        if len(result) > 0:
            return result[0][0]
        return []

    def get_max_temperature_timestamps(self, city, temperature):
        """Return timestamps of maximum temperature. Why timestamps?

        Args:
            city (_type_): _description_
            temperature (_type_): _description_

        Returns:
            _type_: _description_
        """
        query = get_max_historical_temperature_timestamps_query(
            city=city, temperature=temperature, db_type=self._db_type
        )
        result = self.client.execute_query(query_string=query)
        if len(result) > 0:
            return result[0]
        return []

    def get_all_max_temperatures(self):
        """Get maximal historical temperatures for all available cities.

        Returns:
            _type_: _description_
        """
        query = get_all_max_historical_temperatures_query(db_type=self._db_type)
        return self.client.execute_query(query_string=query)

    def get_current_temperature(self, city: str):
        """Get current temperature for city.

        Args:
            city (str): _description_

        Returns:
            _type_: _description_
        """
        if self.city_has_current_temperature(city):
            query = get_current_temperature_query(city=city, db_type=self._db_type)
            result = self.client.execute_query(query_string=query)
            if len(result) > 0:
                return result[0]
        return []

    def get_max_historical_temperature_id(self) -> int:
        """Get max historical temperature id

        Returns:
            _type_: _description_
        """
        query = get_max_historical_temperature_id(self._db_type)
        result = self.client.execute_query(query_string=query)
        if result[0][0] is not None:
            return result[0][0]
        # always wanna start with 1 otherwise
        return 0

    def get_historical_temperature_deltas(self):
        """Example Code:
        cities = self.get_all_cities_from_database()
        cities = [4,2,7,5]
        N = len(cities)
        indexes = [x for x in range(len(cities))]
        c_1_city_indexes = indexes[:(N-1)]
        abs_calcs = []
        start = 0
        for i in c_1_city_indexes:
            for j in indexes[(start + 1):N]:
                abs_calcs.append((i,j))
            start += 1
        """
        pass

    def flush_table(self, table_name):
        """Remove all entries from table."""
        query = flush_table_query(table_name=table_name)
        self.client.execute_query(query_string=query, commit=True)

    def remove_duplicated_historical_entries(self, city):
        """_summary_

        Args:
            city (_type_): _description_
        """
        city_id = get_city_id_from_info(city)
        query = remove_entries_with_duplicated_timestamps(
            self._db_type, city_id=city_id
        )
        self.client.execute_query(query_string=query, commit=True)


class BigQueryClient(DBClient):
    """Client class for bigquery"""

    def __init__(self, project_id: str, dataset: str):
        setup_bigquery_environment("weather-report-406515-20573f2148d1.json")
        self.project_id = project_id
        self.dataset = dataset
        self.client = bigquery.Client(project=project_id)
        self.default_job_config = bigquery.QueryJobConfig(
            default_dataset=project_id + "." + dataset
        )

    def execute_query(
        self, query_string: str, params: dict = None, commit: bool = False
    ):
        """Bigquery specific execution of SQL query.

        Args:
            query_string (str): SQL statement.
            params (dict, optional): SQL params. Defaults to None.
            commit (bool): if False query is read operation and returns values.

        Returns:
            _type_: _description_
        """
        if params is not None:
            query_string = query_string % params
        job = self.client.query(query=query_string, job_config=self.default_job_config)
        result = job.result()
        output = []
        if not commit:
            for r in result:
                output.append(r)
        return output


class MySQLClient(DBClient):
    """Wrapper around MySQL connector"""

    def __init__(self, db_type, host="localhost", user="root"):
        passwd = get_connection_passwd_info(db_type)
        database = get_database_name_info(db_type)
        self.client = mysql.connector.connect(
            host=host, user=user, password=passwd, database=database
        )

    def execute_query(
        self, query_string: str, params: str = None, commit: bool = False
    ) -> list:
        """MySQL specific execution of query.

        Args:
            query_string (str): _description_
            params (str, optional): _description_. Defaults to None.
            commit (bool, optional): _description_. Defaults to False.

        Returns:
            list: _description_
        """
        cursor = self.client.cursor()
        try:
            cursor.execute(query_string, params=params)
        except mysql.connector.Error as err:
            print(
                f"MYSQL ERROR: {err.msg}, ERROR-CODE-FLAG: {get_errorcode_flag(err.errno)}"
            )
        result = []
        if commit:
            # create, update of drop operation
            self.client.commit()
        else:
            # read operation
            try:
                for r in cursor:
                    result.append(r)
            except mysql.connector.errors.InterfaceError:
                pass
            cursor.close()
        return result


class CSVWrapper:
    """Class for generating csv file to be uploaded to bigquery"""

    def create_historical_temperature_file(
        self,
        timestamps: list,
        temperatures: list,
        city: str,
        filename: str,
        start_uuid: int = 1,
    ):
        """Creates historical temperature data in tabular form."""
        table_info = get_table_info()
        city_id = get_city_id_from_info(city)
        data = []
        columns = table_info[get_table_name_historical_temperature("bigquery")][
            "bigquery"
        ].keys()
        uuid = start_uuid
        for s_time, temperature in zip(timestamps, temperatures):
            if temperature is not None:
                data.append((uuid, city_id, s_time, temperature))
                uuid += 1
        df = pd.DataFrame(columns=columns, data=data)
        if pexists(filename):
            _df = pd.read_csv(filename)
            df = pd.concat([_df, df])
        df.to_csv(filename, columns=columns, index=False)


class BigQueryWrapper(DBWrapper):
    """Wrapper around bigquery connector."""

    def __init__(
        self,
        client: BigQueryClient,
    ):
        super().__init__(client)
        self._db_type = "bigquery"

    def _extend_table_name(self, table_name):
        return f"`{self.client.project_id}.{self.client.dataset}.{table_name}`"

    def drop_table(self, table_name):
        # table_name = self._extend_table_name(table_name)
        self.client.execute_query(
            query_string=drop_table_query(table_name=table_name), params=None
        )

    def load_csv_data(self, table_name: str, filename: str) -> None:
        """Upload csv-file to bigquery table. This is much faster than directly
        calling SQL INSERT.

        Args:
            table_name (_type_): _description_
            filename (_type_): _description_
        """
        table_info = get_table_info()
        column_data = table_info[table_name][self._db_type]
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
        lj = self.client.client.load_table_from_file(
            open(filename, "rb"),
            destination=self.client.project_id
            + "."
            + self.client.dataset
            + "."
            + table_name,
            job_config=jc,
        )
        lj.result()

    def flush_table(self, table_name):
        """Remove all entries from table."""
        query = flush_table_query(table_name=table_name)
        self.client.execute_query(query_string=query, commit=True)


class MySQLWrapper(DBWrapper):
    """Wrapper around MySQL connector."""

    def __init__(self, client: MySQLClient):
        super().__init__(client=client)
        self._db_type = "mysql"
