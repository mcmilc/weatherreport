import sys
import getopt
from weatherreport.utilities.helpers import build_date
from weatherreport.transforms.filters import filter_historical_temperature
from weatherreport.weatherAPI.weatherClient import weatherClientFactory
from weatherreport.database.dbAPI import DBAPIFactory


def parse_date_arg(input_date):
    return [int(x) for x in input_date.split("_")]


def main():
    wc = weatherClientFactory()
    input_args = sys.argv[1:]
    optlist, args = getopt.getopt(input_args, "s:e:c:i:d:")
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

    dbAPI = DBAPIFactory(db_type)
    # extract
    data = wc.get_historical_temperature(
        start_date=start_date, end_date=end_date, city=city, interval=interval
    )
    # transform
    timestamps, temperature = filter_historical_temperature(data, interval)
    # load
    dbAPI.create_historical_temperature_file(
        timestamps=timestamps, temperature=temperature, city=city
    )


if __name__ == "__main__":
    main()
