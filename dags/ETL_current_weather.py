import pendulum
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator

from weatherreport.database.dbAPI import DBAPIFactory
from weatherreport.transforms.filters import select_current_temperature
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
    dag_id="ET_current_weather",
    default_args=default_args,
    description="Update current southbay temperature",
    schedule=dt.timedelta(minutes=5),
)


def update_current():
    wc = weatherClientFactory()
    mysqlAPI = DBAPIFactory()
    for city in get_all_city_names():
        data = wc.get_current_temperature(city=city)
        # transform
        timestamp, temperature = select_current_temperature(data)
        # load
        mysqlAPI.populate_current_temperature(
            timestamp=timestamp, temperature=temperature, city=city
        )


update_current_temperature = PythonOperator(
    task_id="update_current_temperature",
    python_callable=update_current,
    dag=dag,
)

update_current
