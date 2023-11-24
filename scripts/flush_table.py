import sys
import getopt
from southbayweather.database.dbAPI import MySQLAPIFactory


def main():
    input_args = sys.argv[1:]
    optlist, args = getopt.getopt(input_args, "t:")
    for opt, arg in optlist:
        if opt == "-t":
            table_name = arg
    mysqlAPI = MySQLAPIFactory()
    mysqlAPI.flush_table(table_name)


if __name__ == "__main__":
    main()
