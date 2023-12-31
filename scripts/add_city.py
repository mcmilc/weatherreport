"""Script to upload city data to database."""
import sys
import getopt

from weatherreport.database.dbAPI import db_wrapper_factory


def main():
    """Usage:
    python3 add_city.py -d [OPTIONS] [PARAMETERS]

    OPTIONS and PARAMETERS:
    -d mysql or bigquery
    """
    input_args = sys.argv[1:]
    optlist, _ = getopt.getopt(input_args, "d:")
    for opt, arg in optlist:
        if opt == "-d":
            db_type = arg

    db_wrapper = db_wrapper_factory(db_type)
    db_wrapper.load_city()


if __name__ == "__main__":
    main()
