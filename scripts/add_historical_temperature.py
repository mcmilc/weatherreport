"""Script for uploading historical temperature data to database."""
import sys
import getopt
from weatherreport.utilities.helpers import build_date
from weatherreport.utilities.helpers import parse_date_arg
from weatherreport.data.helpers import get_table_name_historical_temperature
from weatherreport.transforms.selectors import select_historical_temperature
from weatherreport.weatherAPI.weatherClient import weather_client_factory
from weatherreport.database.dbAPI import db_wrapper_factory
from weatherreport.database.dbAPI import CSVWrapper


def main():
    """Usage:
    python3 add_historical_temperature.py [OPTIONS] [PARAMETERS]

    OPTIONS and PARAMETERS:
    -s start date in format yyyy_mm_dd e.g. 2023_11_1
    -e end date in format yyyy_mm_dd e.g. 2023_11_29
    -c city in quotes e.g. 'Redondo Beach'
    -i either hourly or daily interval between temperature entries
    -d databse i.e. either mysql or bigquery
    -r recreate table flag
    """
    wc = weather_client_factory()
    input_args = sys.argv[1:]
    optlist, _ = getopt.getopt(input_args, "s:e:c:i:d:r:f")
    filename = ""
    recreate_table = False
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
        elif opt == "-d":
            db_type = arg
        elif opt == "-r":
            recreate_table = arg
        elif opt == "-f":
            filename = arg

    table_name = get_table_name_historical_temperature(db_type)

    if db_type == "bigquery" and filename == "":
        raise getopt.GetoptError("Argument -f with filename required for bigquery")

    db_wrapper = db_wrapper_factory(db_type)
    # extract
    data = wc.get_historical_temperature(
        start_date=start_date, end_date=end_date, city=city, interval=interval
    )
    # transform
    timestamps, temperatures = select_historical_temperature(data, interval)
    # load
    if recreate_table == "1":
        db_wrapper.drop_table(table_name)
        db_wrapper.create_table(table_name)
    if db_type == "bigquery":
        csv_wrapper = CSVWrapper()
        csv_wrapper.create_historical_temperature_file(
            timestamps=timestamps,
            temperatures=temperatures,
            city=city,
            filename=filename,
        )
    elif db_type == "mysql":
        db_wrapper.load_historical_temperature(
            timestamps=timestamps, temperatures=temperatures, city=city
        )


if __name__ == "__main__":
    main()
