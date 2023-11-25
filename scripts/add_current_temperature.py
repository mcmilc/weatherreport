import sys
import getopt

from southbayweather.database.dbAPI import MySQLAPIFactory
from southbayweather.transforms.filters import filter_current_temperature
from southbayweather.weatherAPI.weatherClient import weatherClientFactory


def parse_date_arg(input_date):
    return [int(x) for x in input_date.split("_")]


def main():
    wc = weatherClientFactory()
    input_args = sys.argv[1:]
    optlist, args = getopt.getopt(input_args, "c:")
    for opt, arg in optlist:
        if opt == "-c":
            city = arg
    # extract
    data = wc.get_current_temperature(city=city)
    # transform
    timestamp, temperature = filter_current_temperature(data)
    mysqlAPI = MySQLAPIFactory()
    # load
    mysqlAPI.populate_current_temperature(
        timestamp=timestamp, temperature=temperature, city=city
    )


if __name__ == "__main__":
    main()
