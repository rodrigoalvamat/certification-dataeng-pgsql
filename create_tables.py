# system libs
import os
import sys
from timeit import default_timer as timer

# sql libs
import psycopg2
from sql_queries import *


def create_database(cloud):
    """
    Creates and connects to the database.

    Args:
    
    `cloud`: Use the cloud database connection instead of local
    
    Returns:

    The connection and cursor to the database
    """

    if cloud:
        # cloud database connection configuration
        host = os.environ['PGSQL_CLOUD_HOST']
        username = os.environ['PGSQL_CLOUD_USERNAME'] 
        password = os.environ['PGSQL_CLOUD_PASSWORD']
        sparkify_db = f"host={host} dbname={username} user={username} password={password}"
    else:
        # local database connection configuration
        default_db = "host=127.0.0.1 dbname=studentdb user=student password=student"
        sparkify_db = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
        
        try:
            # connect to default database
            conn = psycopg2.connect(default_db)
            conn.set_session(autocommit=True)
            cur = conn.cursor()
            # create sparkify database with UTF8 encoding
            cur.execute("DROP DATABASE IF EXISTS sparkifydb")
            cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
            # close connection to default database
            conn.close()
        except psycopg2.Error as e:
            print("Error: Could not connect to the default database")
            print(e)

    # connect to sparkify database
    conn = psycopg2.connect(sparkify_db)
    cur = conn.cursor()

    return cur, conn


def drop_tables(cursor, connection):
    """
    Drops each table using the queries in `drop_table_queries` list.

    Args:
    
    `cursor`: The database connection cursor

    `connection`: The database connection
    """
    try:    
        for query in drop_table_queries:
            cursor.execute(query)
            connection.commit()
    except psycopg2.Error as e:
        print("Error: Could not execute drop table queries")
        print(e)

def drop_functions(cursor, connection):
    """
    Drops each function using the queries in `drop_function_queries` list.

    Args:
    
    `cursor`: The database connection cursor

    `connection`: The database connection
    """
    try:
        for query in drop_function_queries:
            cursor.execute(query)
            connection.commit()
    except psycopg2.Error as e:
        print("Error: Could not execute drop function queries")
        print(e)


def drop_types(cursor, connection):
    """
    Drops each type using the queries in `drop_type_queries` list.

    Args:
    
    `cursor`: The database connection cursor

    `connection`: The database connection
    """
    try:
        for query in drop_type_queries:
            cursor.execute(query)
            connection.commit()
    except psycopg2.Error as e:
        print("Error: Could not execute drop type queries")
        print(e)


def create_functions(cursor, connection):
    """
    Creates each function using the queries in `create_function_queries` list. 

    Args:
    
    `cursor`: The database connection cursor

    `connection`: The database connection
    """
    try:
        for query in create_function_queries:
            cursor.execute(query)
            connection.commit()
    except psycopg2.Error as e:
        print("Error: Could not execute create function queries")
        print(e)


def create_tables(cursor, connection):
    """
    Creates each table using the queries in `create_table_queries` list. 

    Args:
    
    `cursor`: The database connection cursor

    `connection`: The database connection
    """
    try:
        for query in create_table_queries:
            cursor.execute(query)
            connection.commit()
    except psycopg2.Error as e:
        print("Error: Could not execute create table queries")
        print(e)


def create_types(cursor, connection):
    """
    Creates each type using the queries in `create_type_queries` list. 

    Args:
    
    `cursor`: The database connection cursor

    `connection`: The database connection
    """
    try:
        for query in create_type_queries:
            cursor.execute(query)
            connection.commit()
    except psycopg2.Error as e:
        print("Error: Could not execute create type queries")
        print(e)


def main(cloud):
    """
    - Selects the database host from the command line argument --cloud.
    - Drops (if exists) and Creates the sparkify database. 
    - Establishes connection with the sparkify database and gets cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed. 
    - Finally, closes the connection.

    Args:

    `cloud`: Use the cloud database connection instead of local.
    """
    # initialize the timer
    start = timer()
    
    try:
        # create a cursor and connect to the database
        cur, conn = create_database(cloud)
        # execute all sql statements
        drop_tables(cur, conn)
        drop_functions(cur, conn)
        drop_types(cur, conn)
        create_types(cur, conn)
        create_tables(cur, conn)
        create_functions(cur, conn)
        # close the connection
        conn.close()
    except psycopg2.Error as e:
        print("Error: Could not create a cursor or connect to the database")
        print(e)

    # print the time it took to run the script
    end = timer()
    print(f'Create tables time: {round(end - start, 2)} seconds')


if __name__ == "__main__":
    # calls the main function with the corresponding database host argument
    # getopt or argpase were not used for simplicity
    if len(sys.argv) == 2 and sys.argv[1] == '--cloud':
        main(cloud=True)
    else:
        main(cloud=False)
