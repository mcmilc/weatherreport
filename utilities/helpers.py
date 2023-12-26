"""Helper methods."""

import os
import datetime as dt
from mysql.connector import errorcode

# CONFIG
from weatherreport.config.config import weather_report_root
from weatherreport.utilities.filesystem_utils import pjoin


def build_date(year: int, month: int, day: int) -> str:
    return dt.date(year=year, month=month, day=day).strftime("%Y-%m-%d")


def setup_bigquery_environment(service_account_file):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = pjoin(
        weather_report_root,
        "database",
        service_account_file,
    )


def get_errorcode_flag(code):
    flag = [x for x in dir(errorcode) if getattr(errorcode, x) == code]
    if len(flag) == 1:
        return flag[0]


def parse_date_arg(input_date):
    """Convert year, month, day parts of date string into integers."""
    return [int(x) for x in input_date.split("_")]
