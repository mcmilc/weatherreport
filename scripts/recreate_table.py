import sys
import getopt

from weatherreport.database.dbAPI import DBAPIFactory


def main():
    input_args = sys.argv[1:]
    optlist, args = getopt.getopt(input_args, "d:t:")
    for opt, arg in optlist:
        if opt == "-d":
            db_type = arg
        elif opt == "-t":
            table_name = arg

    dbAPI = DBAPIFactory(db_type)
    dbAPI.drop_table(table_name)
    dbAPI.create_table(table_name)


if __name__ == "__main__":
    main()
