"""DAG that uploads current temperature to database.

Check workflows in this tutorial to run airtflow webserver/scheduler:

https://medium.com/@abraham.pabbathi/airflow-on-aws-ec2-instance-with-ubuntu-aff8d3206171
"""

import datetime as dt
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

# HELPERS
from weatherreport.data.helpers import get_database_access_info

# OPS
from weatherreport.pipelines.current_temperature_ops import _initialize_temp_folder
from weatherreport.pipelines.current_temperature_ops import _extract_data
from weatherreport.pipelines.current_temperature_ops import _transform_timestamp
from weatherreport.pipelines.current_temperature_ops import _transform_temperature
from weatherreport.pipelines.current_temperature_ops import _load_data_to_db
from weatherreport.pipelines.current_temperature_ops import _clean_up

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
    dag_id="ETL_current_weather",
    default_args=default_args,
    description="Update current southbay temperature",
    schedule=dt.timedelta(minutes=15),
    start_date=pendulum.today("UTC").add(days=0),
    catchup=False,
) as dag:
    # Extract

    initialize = PythonOperator(
        task_id="initialize", python_callable=_initialize_temp_folder
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
