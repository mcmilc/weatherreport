import sys
import getopt
from weatherreport.database.dbAPI import DBAPIFactory


def main():
    input_args = sys.argv[1:]
    optlist, args = getopt.getopt(input_args, "t:f:")
    for opt, arg in optlist:
        if opt == "-t":
            table_name = arg
        elif opt == "-f":
            filename = arg
    dbAPI = DBAPIFactory("bigquery")
    dbAPI.drop_table(table_name)
    dbAPI.create_table(table_name)
    dbAPI.upload_csv_data(table_name=table_name, filename=filename)


if __name__ == "__main__":
    main()
