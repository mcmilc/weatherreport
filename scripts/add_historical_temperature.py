"""Script for uploading historical temperature data to database."""
import sys
import getopt
from weatherreport.utilities.helpers import build_date
from weatherreport.utilities.helpers import parse_date_arg
from weatherreport.transforms.selectors import select_historical_temperature
from weatherreport.weatherAPI.weatherClient import weatherClientFactory
from weatherreport.database.dbAPI import db_wrapper_factory
from weatherreport.database.dbAPI import CSVWrapper


def main():
    wc = weatherClientFactory()
    input_args = sys.argv[1:]
    optlist, _ = getopt.getopt(input_args, "s:e:c:i:d:r:f")
    filename = ""
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

    if db_type == "bigquery" and filename == "":
        raise getopt.GetoptError("Argument -f with filename required for bigquery")

    db_wrapper = db_wrapper_factory(db_type)
    # extract
    data = wc.get_historical_temperature(
        start_date=start_date, end_date=end_date, city=city, interval=interval
    )
    # transform
    timestamps, temperature = select_historical_temperature(data, interval)
    # load
    if db_type == "bigquery":
        csv_wrapper = CSVWrapper()
        csv_wrapper.create_historical_temperature_file(
            timestamps=timestamps,
            temperatures=temperature,
            city=city,
            filename=filename,
        )


if __name__ == "__main__":
    main()
