import json
import datetime as dt


def build_date(year: int, month: int, day: int) -> str:
    return dt.date(year=year, month=month, day=day).strftime("%Y-%m-%d")


def read_json(filename: str) -> dict:
    return json.load(open(filename))
