"""DAG that uploads current temperature to database."""
import os
import json
import datetime as dt
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from weatherreport.config.config import WR_TMPDIR
from weatherreport.utilities.filesystem_utils import pjoin
from weatherreport.utilities.filesystem_utils import pexists
from weatherreport.utilities.helpers import build_date

# HELPERS
from weatherreport.data.helpers import read_json_file
from weatherreport.data.helpers import get_database_access_info
from weatherreport.data.helpers import append_suffix
from weatherreport.data.helpers import get_all_city_names
from weatherreport.data.helpers import generate_temp_filename
from weatherreport.data.helpers import remove_temporary_file
from weatherreport.data.helpers import get_table_name_historical_temperature
from weatherreport.data.helpers import to_json

from weatherreport.transforms.converters import convert_timestamp
from weatherreport.transforms.converters import round_float_to_int
from weatherreport.transforms.filters import filter_temperature_by_time

# API
from weatherreport.database.dbAPI import db_wrapper_factory
from weatherreport.database.dbAPI import CSVWrapper
from weatherreport.weatherAPI.weatherClient import weather_client_factory

# Transforms
from weatherreport.transforms.selectors import select_historical_temperature


default_args = {
    "owner": get_database_access_info("airflow")["owner"],
    "start_date": pendulum.today("UTC").add(days=0),
    "email": "matthias.milczynski@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

_fs_metadata = "_historical_metadata.json"


def initialize_temp_folder(**kwargs):
    ti = kwargs["ti"]
    if not pexists(pjoin(os.environ[WR_TMPDIR], _fs_metadata)):
        tmp_timestamps = generate_temp_filename()
        tmp_temperatures = generate_temp_filename()
        _metadata = {
            "tmp_timestamps": tmp_timestamps,
            "tmp_temperatures": tmp_temperatures,
        }
        with open(pjoin(os.environ[WR_TMPDIR], _fs_metadata), "w") as fp:
            json.dump(_metadata, fp)
    else:
        with open(pjoin(os.environ[WR_TMPDIR], _fs_metadata), "r") as fp:
            _metadata = json.load(fp=fp)
            tmp_timestamps = _metadata["tmp_timestamps"]
            tmp_temperatures = _metadata["tmp_temperatures"]

    cities = get_all_city_names()
    params = {
        "db_type": "bigquery",
        "cities": cities,
        "tmp_temperatures": tmp_temperatures,
        "tmp_timestamps": tmp_timestamps,
    }
    data_string = json.dumps(params)
    ti.xcom_push("params", data_string)


def _extract_data(**kwargs):
    """_summary_

    Args:
        params (_type_): _description_
        test_mode (_type_, optional): _description_. Defaults to None.
        task (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="initialize", key="params")
    params = json.loads(params)
    wc = weather_client_factory()
    cities = params["cities"]
    tmp_timestamps = params["tmp_timestamps"]
    tmp_temperatures = params["tmp_temperatures"]

    db_wrapper = db_wrapper_factory(params["db_type"])
    for city in cities:
        latest_timestamp = db_wrapper.get_latest_historical_temperature_timestamp(city)
        if latest_timestamp is not None:
            year, month, day = [
                int(x) for x in latest_timestamp.strftime("%Y %m %d").split()
            ]
            start_date = build_date(year, month, day)
            year, month, day = [
                int(x)
                for x in pendulum.today(tz="local")
                .add(days=-2)
                .strftime("%Y %m %d")
                .split()
            ]
            end_date = build_date(year, month, day)
            data = wc.get_historical_temperature(
                start_date=start_date, end_date=end_date, city=city, interval="hourly"
            )
            timestamp, temperature = select_historical_temperature(data)
            _tmp_timestamps = append_suffix(
                filename=tmp_timestamps, suffix="_hist_" + city
            )
            _tmp_temperatures = append_suffix(
                filename=tmp_temperatures, suffix="_hist_" + city
            )
            to_json(timestamp, _tmp_timestamps)
            to_json(temperature, _tmp_temperatures)


# Transform timestamps
def _transform_timestamps(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="initialize", key="params")
    params = json.loads(params)
    cities = params["cities"]
    tmp_timestamps = params["tmp_timestamps"]
    for city in cities:
        _tmp_timestamps = append_suffix(filename=tmp_timestamps, suffix="_hist_" + city)
        try:
            data = read_json_file(_tmp_timestamps)
            transformed_data = [convert_timestamp(d) for d in data if d is not None]
            to_json(transformed_data, _tmp_timestamps)
        except FileNotFoundError:
            pass


# Transform temperature
def _transform_temperatures(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="initialize", key="params")
    params = json.loads(params)
    cities = params["cities"]
    tmp_temperatures = params["tmp_temperatures"]
    for city in cities:
        _tmp_temperatures = append_suffix(
            filename=tmp_temperatures, suffix="_hist_" + city
        )
        try:
            data = read_json_file(_tmp_temperatures)
            transformed_data = [round_float_to_int(d) for d in data if d is not None]
            to_json(transformed_data, _tmp_temperatures)
        except FileNotFoundError:
            pass


# Load data
def _load_data_to_db(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="initialize", key="params")
    params = json.loads(params)
    db_type = params["db_type"]
    db_wrapper = db_wrapper_factory(db_type)
    cities = params["cities"]
    tmp_timestamps = params["tmp_timestamps"]
    tmp_temperatures = params["tmp_temperatures"]
    for city in cities:
        _tmp_timestamps = append_suffix(filename=tmp_timestamps, suffix="_hist_" + city)
        _tmp_temperatures = append_suffix(
            filename=tmp_temperatures, suffix="_hist_" + city
        )
        try:
            timestamps = read_json_file(_tmp_timestamps)
            temperatures = read_json_file(_tmp_temperatures)
            if db_type == "mysql":
                db_wrapper.load_historical_temperature(
                    timestamps=timestamps, temperatures=temperatures, city=city
                )
            elif db_type == "bigquery":
                csv_wrapper = CSVWrapper()
                start_uuid = db_wrapper.get_max_historical_temperature_id(city)
                baseline_timestamp = (
                    db_wrapper.get_latest_historical_temperature_timestamp(city)
                )
                filename = pjoin(os.getenv(WR_TMPDIR), "_bq_query_data.csv")
                if baseline_timestamp is not None:
                    timestamps, temperatures = filter_temperature_by_time(
                        timestamps=timestamps,
                        temperatures=temperatures,
                        baseline_timestamp=baseline_timestamp,
                    )
                csv_wrapper.create_historical_temperature_file(
                    timestamps=timestamps,
                    temperatures=temperatures,
                    city=city,
                    filename=filename,
                    start_uuid=start_uuid + 1,
                )
                table_name = get_table_name_historical_temperature("bigquery")
                db_wrapper.load_csv_data(table_name=table_name, filename=filename)
        except FileNotFoundError:
            pass


def _clean_up():
    for f in os.listdir(os.environ[WR_TMPDIR]):
        remove_temporary_file(pjoin(os.environ[WR_TMPDIR], f))


with DAG(
    dag_id="ETL_historical_weather",
    default_args=default_args,
    description="Update current southbay temperature",
    schedule=dt.timedelta(days=2),
    start_date=pendulum.today("UTC").add(days=0),
    catchup=False,
) as dag:
    # Extract

    initialize = PythonOperator(
        task_id="initialize", python_callable=initialize_temp_folder
    )

    extract_data = PythonOperator(task_id="extract_data", python_callable=_extract_data)
    transform_timestamps = PythonOperator(
        task_id="transform_timestamps", python_callable=_transform_timestamps
    )
    transform_temperatures = PythonOperator(
        task_id="transform_temperatures", python_callable=_transform_temperatures
    )
    load_data_to_db = PythonOperator(
        task_id="load_data_to_db", python_callable=_load_data_to_db
    )
    clean_up = PythonOperator(task_id="clean_up", python_callable=_clean_up)
    (
        initialize
        >> extract_data
        >> transform_timestamps
        >> transform_temperatures
        >> load_data_to_db
        >> clean_up
    )
