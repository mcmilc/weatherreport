"""DAG that uploads historical temperature to database."""
import datetime as dt
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from weatherreport.database.dbAPI import db_wrapper_factory
from weatherreport.transforms.selectors import select_historical_temperature
from weatherreport.database.queries import get_all_city_names
from weatherreport.weatherAPI.weatherClient import weatherClientFactory

default_args = {
    "owner": "Matthias Milczynski",
    "start_date": pendulum.today("UTC").add(days=0),
    "email": "matthias.milczynski@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

dag = DAG(
    dag_id="ETL_historical_weather",
    default_args=default_args,
    description="Update historical southbay temperature",
    schedule=dt.timedelta(days=1),
    params={"db_type": "mysql"},
)


def _upload_current_temperature(**context):
    """Callback that uploads current temperature of a city to database."""
    wc = weatherClientFactory()
    db_wrapper = db_wrapper_factory(context["params"]["db_type"])
    for city in get_all_city_names():
        data = wc.get_current_temperature(city=city)
        # transform
        timestamps, temperatures = select_historical_temperature(data)
        # load
        db_wrapper.upload_historical_temperature(
            timestamps=timestamps, temperatures=temperatures, city=city
        )


upload_current_temperature = PythonOperator(
    task_id="update_current_temperature",
    python_callable=_upload_current_temperature,
    dag=dag,
)

_upload_current_temperature
