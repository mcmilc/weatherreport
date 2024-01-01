"""DAG that uploads current temperature to database."""

import datetime as dt
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

# HELPERS
from weatherreport.data.helpers import get_database_access_info

# OPS
from weatherreport.pipelines.historical_temperature_ops import _initialize_temp_folder
from weatherreport.pipelines.historical_temperature_ops import _extract_data
from weatherreport.pipelines.historical_temperature_ops import _transform_timestamps
from weatherreport.pipelines.historical_temperature_ops import _transform_temperatures
from weatherreport.pipelines.historical_temperature_ops import _load_data_to_db
from weatherreport.pipelines.historical_temperature_ops import _clean_up

default_args = {
    "owner": get_database_access_info("airflow")["owner"],
    "start_date": pendulum.today("UTC").add(days=0),
    "email": "matthias.milczynski@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}


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
        task_id="initialize", python_callable=_initialize_temp_folder
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
