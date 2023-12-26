"""DAG that uploads current temperature to database."""
import os
import json
import datetime as dt
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from weatherreport.config.config import ENV_VAR_NAME
from weatherreport.utilities.filesystem_utils import pjoin
from weatherreport.utilities.filesystem_utils import pexists

# HELPERS
from weatherreport.data.helpers import read_json_file
from weatherreport.data.helpers import append_suffix_to_filename
from weatherreport.data.helpers import get_all_city_names
from weatherreport.data.helpers import generate_temp_filename
from weatherreport.data.helpers import remove_temporary_file

from weatherreport.transforms.converters import convert_timestamp
from weatherreport.transforms.converters import round_float_to_int

# API
from weatherreport.database.dbAPI import db_wrapper_factory
from weatherreport.weatherAPI.weatherClient import weather_client_factory

# Transforms
from weatherreport.transforms.selectors import select_current_temperature


default_args = {
    "owner": "Matthias Milczynski",
    "start_date": pendulum.today("UTC").add(days=0),
    "email": "matthias.milczynski@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

_tmp_files_metadata = "_metadata.json"


def _prepare_filename(filename, suffix):
    return append_suffix_to_filename(filename, suffix)


def initialize_temp_folder(**kwargs):
    ti = kwargs["ti"]
    if not pexists(pjoin(os.environ[ENV_VAR_NAME], _tmp_files_metadata)):
        tmp_timestamp_file = generate_temp_filename()
        tmp_temperature_file = generate_temp_filename()
        _metadata = {
            "tmp_timestamp_file": tmp_timestamp_file,
            "tmp_temperature_file": tmp_temperature_file,
        }
        with open(pjoin(os.environ[ENV_VAR_NAME], _tmp_files_metadata), "w") as fp:
            json.dump(_metadata, fp)
    else:
        with open(pjoin(os.environ[ENV_VAR_NAME], _tmp_files_metadata), "r") as fp:
            _metadata = json.load(fp=fp)
            tmp_timestamp_file = _metadata["tmp_timestamp_file"]
            tmp_temperature_file = _metadata["tmp_temperature_file"]

    cities = get_all_city_names()
    params = {
        "db_type": "mysql",
        "cities": cities,
        "tmp_temperature_file": tmp_temperature_file,
        "tmp_timestamp_file": tmp_timestamp_file,
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
    tmp_timestamp_file = params["tmp_timestamp_file"]
    tmp_temperature_file = params["tmp_temperature_file"]
    for city in cities:
        data = wc.get_current_temperature(city=city)
        timestamp, temperature = select_current_temperature(data)
        _tmp_timestamp_file = _prepare_filename(
            filename=tmp_timestamp_file, suffix="_" + city
        )
        _tmp_temperature_file = _prepare_filename(
            filename=tmp_temperature_file, suffix="_" + city
        )
        # store_as_json_to_tempdir(timestamp, _tmp_timestamp_file)
        # store_as_json_to_tempdir(temperature, _tmp_temperature_file)
        with open(_tmp_timestamp_file, "wt") as fp:
            fp.write(timestamp)
        with open(_tmp_temperature_file, "wt") as fp:
            fp.write(str(temperature))


# Transform timestamps
def _transform_timestamp(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="initialize", key="params")
    params = json.loads(params)
    cities = params["cities"]
    tmp_timestamp_file = params["tmp_timestamp_file"]
    for city in cities:
        _tmp_timestamp_file = _prepare_filename(
            filename=tmp_timestamp_file, suffix="_" + city
        )
        with open(_tmp_timestamp_file, "rt") as fp:
            data = fp.read()
        transformed_data = convert_timestamp(data)
        with open(_tmp_timestamp_file, "wt") as fp:
            fp.write(transformed_data)


# Transform temperature
def _transform_temperature(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="initialize", key="params")
    params = json.loads(params)
    cities = params["cities"]
    tmp_temperature_file = params["tmp_temperature_file"]
    for city in cities:
        _tmp_temperature_file = _prepare_filename(
            filename=tmp_temperature_file, suffix="_" + city
        )
        with open(_tmp_temperature_file, "rt") as fp:
            data = float(fp.read())
        transformed_data = round_float_to_int(data)
        with open(_tmp_temperature_file, "wt") as fp:
            fp.write(str(transformed_data))


# Load data
def _load_data_to_db(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="initialize", key="params")
    params = json.loads(params)
    db_wrapper = db_wrapper_factory(params["db_type"])
    cities = params["cities"]
    tmp_timestamp_file = params["tmp_timestamp_file"]
    tmp_temperature_file = params["tmp_temperature_file"]
    for city in cities:
        _tmp_timestamp_file = _prepare_filename(
            filename=tmp_timestamp_file, suffix="_" + city
        )
        _tmp_temperature_file = _prepare_filename(
            filename=tmp_temperature_file, suffix="_" + city
        )
        with open(_tmp_timestamp_file, "rt") as fp:
            timestamp = fp.read()
        with open(_tmp_temperature_file, "rt") as fp:
            temperature = int(fp.read())
        temperature = read_json_file(_tmp_temperature_file)
        db_wrapper.load_current_temperature(
            timestamp=timestamp, temperature=temperature, city=city
        )


def _clean_up():
    for f in os.listdir(os.environ[ENV_VAR_NAME]):
        remove_temporary_file(pjoin(os.environ[ENV_VAR_NAME], f))


with DAG(
    dag_id="ETL_current_weather",
    default_args=default_args,
    description="Update current southbay temperature",
    schedule=dt.timedelta(minutes=15),
    start_date=pendulum.today("UTC").add(days=0),
    catchup=False,
) as dag:
    # Extract

    initialize = PythonOperator(
        task_id="initialize", python_callable=initialize_temp_folder
    )

    extract_data = PythonOperator(task_id="extract_data", python_callable=_extract_data)
    transform_timestamp = PythonOperator(
        task_id="transform_timestamp", python_callable=_transform_timestamp
    )
    transform_temperature = PythonOperator(
        task_id="transform_temperature", python_callable=_transform_temperature
    )
    load_data_to_db = PythonOperator(
        task_id="load_data_to_db", python_callable=_load_data_to_db
    )
    clean_up = PythonOperator(task_id="clean_up", python_callable=_clean_up)
    (
        initialize
        >> extract_data
        >> transform_timestamp
        >> transform_temperature
        >> load_data_to_db
        >> clean_up
    )
