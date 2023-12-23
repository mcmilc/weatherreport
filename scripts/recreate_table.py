"""Script to recreate a table."""
import sys
import getopt

from weatherreport.database.dbAPI import db_wrapper_factory


def main():
    """Usage:
    python3 recreate_table.py [OPTIONS] [PARAMETER]

    OPTIONS and PARAMETERS:
    -d mysql or bigquery
    -t table name in quotes
    """
    input_args = sys.argv[1:]
    optlist, _ = getopt.getopt(input_args, "d:t:")
    for opt, arg in optlist:
        if opt == "-d":
            db_type = arg
        elif opt == "-t":
            table_name = arg

    db_wrapper = db_wrapper_factory(db_type)
    db_wrapper.drop_table(table_name)
    db_wrapper.create_table(table_name)


if __name__ == "__main__":
    main()
