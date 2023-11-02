import datetime as dt


def build_date(year, month, day):
    return dt.date(year=year, month=month, day=day).strftime("%Y-%m-%d")
