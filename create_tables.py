# system libs
import sys

# sql libs
import psycopg2
from sql_queries import *


def create_database(cloud):
    """
    Creates and connects to the database.
    Arguments:
        cloud: Use the cloud database connection instead of local
    Returns:
        The connection and cursor to the database
    """

    if cloud:
        # cloud database connection configuration
        default_db = "host=heffalump.db.elephantsql.com dbname=erxsjqjd user=erxsjqjd password=OhDRrRbv8b59vECc08ENtqtG3rFekShP"
        sparkify_db = default_db
    else:
        # local database connection configuration
        default_db = "host=127.0.0.1 dbname=studentdb user=student password=student"
        sparkify_db = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
        # connect to default database
        conn = psycopg2.connect(default_db)
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
        # close connection to default database
        conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(sparkify_db)
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def drop_types(cur, conn):
    """
    Drops each type using the queries in `drop_type_queries` list.
    """
    for query in drop_type_queries:
        cur.execute(query)
        conn.commit()


def create_functions(cur, conn):
    """
    Creates each function using the queries in `create_function_queries` list. 
    """
    for query in create_function_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def create_types(cur, conn):
    """
    Creates each type using the queries in `create_type_queries` list. 
    """
    for query in create_type_queries:
        cur.execute(query)
        conn.commit()


def main(cloud):
    """
    - Selects the database host from the command line argument --cloud.
    - Drops (if exists) and Creates the sparkify database. 
    - Establishes connection with the sparkify database and gets cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed. 
    - Finally, closes the connection.

    Arguments:
        cloud: Use the cloud database connection instead of local.
    """
    cur, conn = create_database(cloud)

    drop_tables(cur, conn)
    drop_types(cur, conn)
    create_types(cur, conn)
    create_tables(cur, conn)
    create_functions(cur, conn)

    conn.close()


if __name__ == "__main__":
    # calls the main function with the corresponding database host argument
    # getopt or argpase were not used for simplicity
    if len(sys.argv) == 2 and sys.argv[1] == '--cloud':
        main(cloud=True)
    else:
        main(cloud=False)
