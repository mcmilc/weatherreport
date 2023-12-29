"""Script to create csv file with historical temperature data."""
import sys
import getopt
from weatherreport.utilities.helpers import build_date
from weatherreport.utilities.helpers import parse_date_arg
from weatherreport.transforms.selectors import select_historical_temperature
from weatherreport.transforms.converters import convert_timestamp
from weatherreport.transforms.converters import round_float_to_int
from weatherreport.transforms.filters import filter_temperature_by_time
from weatherreport.weatherAPI.weatherClient import weather_client_factory
from weatherreport.database.dbAPI import CSVWrapper
from weatherreport.database.dbAPI import db_wrapper_factory


def main():
    """Usage:"""
    wc = weather_client_factory()
    input_args = sys.argv[1:]
    optlist, _ = getopt.getopt(input_args, "s:e:c:i:f:")
    for opt, arg in optlist:
        if opt == "-s":
            year, month, day = parse_date_arg(arg)
            start_date = build_date(year, month, day)
        elif opt == "-e":
            year, month, day = parse_date_arg(arg)
            end_date = build_date(year, month, day)
        elif opt == "-c":
            city = arg
        elif opt == "-i":
            interval = arg
        elif opt == "-f":
            filename = arg

    csv_wrapper = CSVWrapper()
    db_wrapper = db_wrapper_factory("bigquery")
    start_uuid = db_wrapper.get_max_historical_temperature_id(city)
    if start_uuid > 1:
        start_uuid += 1
    baseline_timestamp = db_wrapper.get_latest_historical_temperature_timestamp(city)
    # extract
    data = wc.get_historical_temperature(
        start_date=start_date, end_date=end_date, city=city, interval=interval
    )
    # transform
    timestamps, temperatures = select_historical_temperature(data, interval)
    timestamps = [convert_timestamp(t) for t in timestamps]
    temperatures = [round_float_to_int(t) for t in temperatures]
    if baseline_timestamp is not None:
        timestamps, temperatures = filter_temperature_by_time(
            timestamps=timestamps,
            temperatures=temperatures,
            baseline_timestamp=baseline_timestamp,
        )
    # load
    print(
        f"Temperature data has {len(timestamps)} timestamp and {len(temperatures)} temperature entries."
    )
    csv_wrapper.create_historical_temperature_file(
        timestamps=timestamps,
        temperatures=temperatures,
        city=city,
        filename=filename,
        start_uuid=start_uuid,
    )


if __name__ == "__main__":
    main()
