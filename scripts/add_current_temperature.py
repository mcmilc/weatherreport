"""Script to upload historical temperature data into database."""
import sys
import getopt

from weatherreport.database.dbAPI import db_wrapper_factory
from weatherreport.transforms.selectors import select_current_temperature
from weatherreport.weatherAPI.weatherClient import weatherClientFactory
from weatherreport.database.queries import get_current_table


def main():
    """Usage:
    python3 add_current_temperature.py [OPTIONS] [PARAMETERS]

    OPTIONS and PARAMETERS:
    -c city iwth quotes e.g. 'Hawthorne'
    -d mysql or bigquery
    -r recreate table flag 0 (do not recreate) or 1 (recreate)
    """
    wc = weatherClientFactory()
    input_args = sys.argv[1:]
    optlist, _ = getopt.getopt(input_args, "c:d:r:")
    print(optlist)
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
    timestamp, temperature = select_current_temperature(data)
    db_wrapper = db_wrapper_factory(db_type)
    # load
    if re_create == "1":
        db_wrapper.drop_table(get_current_table(db_type))
        db_wrapper.create_table(get_current_table(db_type))
    db_wrapper.upload_current_temperature(
        timestamp=timestamp, temperature=temperature, city=city
    )


if __name__ == "__main__":
    main()
