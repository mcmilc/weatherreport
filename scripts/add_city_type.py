import sys
import getopt

from weatherreport.database.dbAPI import DBAPIFactory


def main():
    input_args = sys.argv[1:]
    optlist, args = getopt.getopt(input_args, "d:")
    for opt, arg in optlist:
        if opt == "-d":
            db_type = arg

    dbAPI = DBAPIFactory(db_type)
    dbAPI.populate_city_type()


if __name__ == "__main__":
    main()
