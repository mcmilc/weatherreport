import sys
import getopt
from weatherreport.database.dbAPI import DBAPIFactory


def main():
    input_args = sys.argv[1:]
    optlist, args = getopt.getopt(input_args, "t:d:")
    for opt, arg in optlist:
        if opt == "-t":
            table_name = arg
        elif opt == "-d":
            db_type = arg

    dbAPI = DBAPIFactory(db_type)
    dbAPI.flush_table(table_name)


if __name__ == "__main__":
    main()
