# system libs
import glob
import os
import sys
from timeit import default_timer as timer

# etl/eda libs
import pandas as pd

# sql libs
import psycopg2
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a JSON song file and executes the insert statements for songs and artists tables.

    Args:

    `cur`: The database connection cursor

    `filepath`: The path of the JSON file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Error: Could not insert artist record")
        print(e)

    # insert song record
    song_data = df[['song_id', 'title', 'year', 'duration', 'artist_id']].values[0].tolist()
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Error: Could not insert song record")
        print(e)
    

def process_log_file(cur, filepath):
    """
    Processes a JSON log file and executes the insert statements for time, users and songplays tables.

    Args:

    `cur`: The database connection cursor

    `filepath`: The path of the JSON file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = df['ts'].apply(lambda x: [x, x.hour, x.day, x.week, x.month, x.year, x.day_name()]).to_list()
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    try:
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
    except psycopg2.Error as e:
        print("Error: Could not insert time record")
        print(e)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    user_df['userId'] = user_df['userId'].astype('int64')

    try:
        # insert user records
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
    except psycopg2.Error as e:
        print("Error: Could not insert user record")
        print(e)

    # insert songplay records
    for index, row in df.iterrows():
        
        try:
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.length, row.artist))
            results = cur.fetchone()
        except psycopg2.Error as e:
            print("Error: Could not select song and artist record")
            print(e)
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.level, row.location, row.sessionId, row.userAgent, row.ts, row.userId, artistid, songid]
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Could not insert songplay record")
            print(e)


def process_data(conn, filepath, func):
    """
    Reads multiple JSON files and executes the corresponding data processing function.

    Args:

    `conn`: The database connection

    `filepath`: The path of the JSON folder root

    `func`: The function to execute for each JSON file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(conn.cursor(), datafile)

        try:
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Could not commit changes to database")
            print(e)

        print('{}/{} files processed.'.format(i, num_files))


def main(cloud):
    """
    - Selects the database host from the command line argument --cloud.
    - Create a new database session.
    - Processes all JSON files from song_data folder.
    - Processes all JSON files from log_data folder.
    - Closes the database session.

    Args:
    
    `cloud`: Use the cloud database connection instead of local
    """
    # initialize the timer
    start = timer()

    if cloud:
        # cloud database connection configuration
        host = os.environ['PGSQL_CLOUD_HOST']
        username = os.environ['PGSQL_CLOUD_USERNAME'] 
        password = os.environ['PGSQL_CLOUD_PASSWORD']
        dbconf = f"host={host} dbname={username} user={username} password={password}"
    else:
        # local database connection configuration
        dbconf = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    
    try:
        # connect to the database
        conn = psycopg2.connect(dbconf)
        # process JSON files
        process_data(conn, filepath='../data/song_data', func=process_song_file)
        process_data(conn, filepath='../data/log_data', func=process_log_file)
        # close the connection
        conn.close()
    except psycopg2.Error as e:
        print("Error: Could not connect to database")
        print(e)

    # print the time it took to run the script
    end = timer()
    print(f'ETL time: {round(end - start, 2)} seconds')


if __name__ == "__main__":
    # calls the main function with the corresponding database host argument
    # getopt or argpase were not used for simplicity
    if len(sys.argv) == 2 and sys.argv[1] == '--cloud':
        main(cloud=True)
    else:
        main(cloud=False)