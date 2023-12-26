"""Script for uploading data from a csv file to a bigquery table."""
import os
import sys
import getopt
from weatherreport.database.dbAPI import db_wrapper_factory


def main():
    """Usage:
    python3 upload_csv_data_to_bigquery.py [OPTIONS] [PARAMETER]

    OPTIONS and PARAMETERS:
    -d mysql or bigquery
    -t table name in quotes
    """
    input_args = sys.argv[1:]
    optlist, _ = getopt.getopt(input_args, "t:f:r")
    reset_table = False
    for opt, arg in optlist:
        if opt == "-t":
            table_name = arg
        elif opt == "-f":
            filename = arg
        elif opt == "-r":
            if arg == 1:
                reset_table = True
    db_wrapper = db_wrapper_factory("bigquery")
    if reset_table:
        db_wrapper.drop_table(table_name)
        db_wrapper.create_table(table_name)
    db_wrapper.load_csv_data(table_name=table_name, filename=filename)
    os.remove(filename)


if __name__ == "__main__":
    main()
