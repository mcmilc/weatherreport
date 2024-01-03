"""_summary_
"""
import time
import getopt
import datetime as dt
import os
import sys
import json
import pendulum

from weatherreport.pipelines.ti_mock import TIMock
from weatherreport.config.config import WR_TMPDIR
from weatherreport.utilities.filesystem_utils import pjoin
from weatherreport.utilities.filesystem_utils import pexists
from weatherreport.utilities.helpers import build_date

# HELPERS
from weatherreport.data.helpers import read_json_file

from weatherreport.data.helpers import append_suffix
from weatherreport.data.helpers import get_all_city_names
from weatherreport.data.helpers import get_city_timezone
from weatherreport.data.helpers import generate_temp_filename
from weatherreport.data.helpers import remove_temporary_file
from weatherreport.data.helpers import get_table_name_historical_temperature
from weatherreport.data.helpers import to_json

from weatherreport.transforms.converters import convert_timestamp_to_utc
from weatherreport.transforms.converters import round_float_to_int
from weatherreport.transforms.filters import filter_temperature_by_time

# API
from weatherreport.database.dbAPI import db_wrapper_factory
from weatherreport.database.dbAPI import CSVWrapper
from weatherreport.weatherAPI.weatherClient import weather_client_factory

# Transforms
from weatherreport.transforms.selectors import select_historical_temperature

_fs_metadata = "_historical_metadata.json"


def _initialize_temp_folder(**kwargs):
    ti = kwargs["ti"]
    city = kwargs["city"]
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
        "cities": [city],
        "tmp_temperatures": tmp_temperatures,
        "tmp_timestamps": tmp_timestamps,
        "start_date": "1950_1_1",
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
        if latest_timestamp is None:
            latest_timestamp = dt.datetime.strptime(params["start_date"], "%Y_%m_%d")
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
        n_retries = 3
        for n in range(n_retries):
            print(
                f"Attempt nr. {n + 1} perform api-request for temperature from {start_date} to {end_date} for {city}."
            )
            data = wc.get_historical_temperature(
                start_date=start_date, end_date=end_date, city=city, interval="hourly"
            )
            if data is not None:
                if "hourly" in data.keys():
                    break
                else:
                    print(f"This is historical data: {data}")
            # add this sleep time to try to resolve minutely API request limit
            time.sleep(61)
        timestamp, temperature = select_historical_temperature(data)
        _tmp_timestamps = append_suffix(filename=tmp_timestamps, suffix="_hist_" + city)
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
        print(f"Transforming timestamps for {city}")
        _tmp_timestamps = append_suffix(filename=tmp_timestamps, suffix="_hist_" + city)
        try:
            data = read_json_file(_tmp_timestamps)
            timezone = get_city_timezone(city)
            transformed_data = [
                convert_timestamp_to_utc(d, timezone) for d in data if d is not None
            ]
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
        print(f"Transforming temperature for {city}")
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
        print(f"Uploading temperature data for {city}.")
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
                start_uuid = db_wrapper.get_max_historical_temperature_id()
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


def _remove_duplicates(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="initialize", key="params")
    params = json.loads(params)
    db_type = params["db_type"]
    db_wrapper = db_wrapper_factory(db_type)
    cities = params["cities"]
    for city in cities:
        print(f"Removing duplicates for {city}.")
        db_wrapper.remove_duplicated_historical_entries(city)


def _clean_up():
    print("Cleaning up temporary data.")
    for f in os.listdir(os.environ[WR_TMPDIR]):
        remove_temporary_file(pjoin(os.environ[WR_TMPDIR], f))


if __name__ == "__main__":
    input_args = sys.argv[1:]
    optlist, _ = getopt.getopt(input_args, "c:")
    for opt, arg in optlist:
        if opt == "-c":
            city = arg
    _ti = TIMock()
    _clean_up()
    _initialize_temp_folder(ti=_ti, city=city)
    _extract_data(ti=_ti)
    _transform_timestamps(ti=_ti)
    _transform_temperatures(ti=_ti)
    _load_data_to_db(ti=_ti)
    _remove_duplicates(ti=_ti)
    _clean_up()
