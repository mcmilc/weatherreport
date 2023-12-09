import sys
import getopt

from weatherreport.database.dbAPI import DBAPIFactory
from weatherreport.transforms.filters import filter_current_temperature
from weatherreport.weatherAPI.weatherClient import weatherClientFactory
from weatherreport.database.queries import get_current_table


def parse_date_arg(input_date):
    return [int(x) for x in input_date.split("_")]


def main():
    wc = weatherClientFactory()
    input_args = sys.argv[1:]
    optlist, args = getopt.getopt(input_args, "c:d:r:")
    for opt, arg in optlist:
        if opt == "-c":
            city = arg
        elif opt == "-d":
            db_type = arg
        elif opt == "-r":
            # drop and create table
            re_create = arg
    # extract
    data = wc.get_current_temperature(city=city)
    # transform
    timestamp, temperature = filter_current_temperature(data)
    dbAPI = DBAPIFactory(db_type)
    # load
    if re_create == "1":
        dbAPI.drop_table(get_current_table(db_type))
        dbAPI.create_table(get_current_table(db_type))
    dbAPI.populate_current_temperature(
        timestamp=timestamp, temperature=temperature, city=city
    )


if __name__ == "__main__":
    main()
