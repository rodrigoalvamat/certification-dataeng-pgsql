# system libs
import glob
from io import StringIO
import os
import sys

# etl/eda libs
import pandas as pd

# sql libs
import psycopg2
from sql_queries import *

# suppress warnings
import warnings
warnings.filterwarnings("ignore")


"""Table column names."""
TABLE = {
    'artists': ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'],
    'songs': ['song_id', 'title', 'year', 'duration', 'artist_id'],
    'time': ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday'],
    'users': ['userId', 'firstName', 'lastName', 'gender', 'level'],
    'songplays': ['level', 'location', 'sessionId', 'userAgent', 'ts', 'userId']
    # songplays ['artist_id', 'song_id'] will JOIN after SQL function call
}


# FILE PATHS

def get_files(dir):
    """
    Get a list of all JSON file paths in a directory and it's subdirectories.
    Arguments:
        folder: The path of the JSON root directory
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(dir):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


# DATA FRAME CONCATENATION OF JSON FILES

def get_dataframe(files):
    """
    Concatenates all JSON files into a sigle DataFrame.
    Arguments:
        files: The list of JSON file paths
    """
    dfs = []
    for f in files:
        df = pd.read_json(f, lines=True)
        dfs.append(df)

    data = pd.concat(dfs, ignore_index=True)

    # trim all strings to avoid query mismatch
    strings = list(data.select_dtypes(include=['object']).columns)
    data[strings] = data[strings].apply(lambda x: x.str.strip())

    return data


# SQL BATCH INSERTS (copy_from for performance)

def copy_data(data, table, cursor, columns=None):
    """
    Processes the data from a DataFrame and copy to the database table.
    Arguments:
        data: The dataframe containing the data
        table: The database table name
        cursor: The database cursor
        columns: Optional list of columns to copy
    """
    print(f'Copying {data.shape[0]} records to table {table}...')

    # creates a CSV buffer from the DataFrame
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False, header=False, sep='|', na_rep='NULL')
    csv_buffer.seek(0)

    # copy records from the CSV buffer to the database table
    cursor.copy_from(csv_buffer, table, sep='|', null='NULL', columns=columns)
    cursor.connection.commit()

    # closes the buffer
    csv_buffer.close()


# TABLE INSERTION PROCESSING

def process_table_artists(data, cursor):
    """
    Processes the artists table data.
    Arguments:
        data: The DataFrame containing artists data
        cursor: The database cursor
    """
    # copy artists records to the database table
    artists_data = data[TABLE['artists']].drop_duplicates(
        subset=['artist_id'], keep='first')
    copy_data(artists_data, 'artists', cursor)


def process_table_songs(data, cursor):
    """
    Processes the songs table data.
    Arguments:
        data: The DataFrame containing songs data
        cursor: The database cursor
    """
    # copy songs records to the database table
    songs_data = data[TABLE['songs']].drop_duplicates(
        subset=['song_id'], keep='first')
    copy_data(songs_data, 'songs', cursor)


def process_table_time(data, cursor):
    """
    Processes the time table data.
    Arguments:
        data: The DataFrame containing time data
        cursor: The database cursor
    """
    # copy time records to the database table
    time_df = pd.DataFrame(data.to_list(), columns=TABLE['time']).drop_duplicates(
        subset=['timestamp'], keep='first')
    copy_data(time_df, 'time', cursor)


def process_table_users(data, cursor):
    """
    Processes the users table data.
    Arguments:
        data: The DataFrame containing users data
        cursor: The database cursor
    """
    # copy users records to the database table
    users_data = data[TABLE['users']].drop_duplicates(
        subset=['userId'], keep='first')
    copy_data(users_data, 'users', cursor)


def process_table_songplays(data, cursor):
    """
    Processes the songplays table data.
    Arguments:
        data: The DataFrame containing songplays data
        cursor: The database cursor
    """
    # copy songplays records to the database table
    columns = ['level', 'location', 'session_id', 'user_agent',
               'start_time', 'user_id', 'artist_id', 'song_id']
    copy_data(data, 'songplays', cursor, columns=columns)


# SQL FUNCTION / STORED PROCEDURE (batch query and fetch for performance)

def get_song_artist_ids(data, cursor):
    """
    Gets the song and artist IDs from the database function.
    Arguments:
        data: The DataFrame containing songplays data
        cursor: The database cursor
    """
    # Query the song_artist_ids function with a list of song, length and artist
    func = "SELECT * FROM song_artist_ids(CAST (%s AS SONG_ARTIST[]))"
    param = data[['song', 'length', 'artist']].to_records(index=True).tolist()

    cursor.execute(func, (param,))

    # fetch the results in batches of 2000
    ids = []
    while True:
        records = cursor.fetchmany(size=2000)
        if not records:
            break
        else:
            ids[-1:-1] = records

    # convert the list of ids to a DataFrame
    ids_df = pd.DataFrame(ids, columns=['index', 'song_id', 'artist_id'])
    ids_df.set_index('index', inplace=True)

    cursor.connection.commit()

    return ids_df


# ETL PROCESSING - DEALING WITH NULL VALUES

def generate_user_id(data):
    """
    Generates a userId for each user with null id.
    Creates an unique key based on the columns first name, last name, gender and level.
    Arguments:
        data: The DataFrame containing users data
    """
    # create an unique key identifier
    data['userKey'] = data.apply(
        lambda x: x['firstName'] + x['lastName'] + x['gender'] + x['level'], axis=1)
    # get the last integer number used as an userId
    maxId = data['userId'].max()
    # get the unique keys generated for the users with null id
    keys = data[data['userId'].isnull()]['userKey'].unique()
    # create a dictionary where the key is the userKey and the value is the next generated userId
    ids = dict(zip(keys, [x for x in range(maxId + 1, maxId + len(keys) + 1)]))
    # replace the null userId with the generated id
    return data.apply(lambda x: ids.get(x['userKey']) if pd.isnull(x['userId']) else x['userId'], axis=1)


# ETL PROCESSING - EXTRACTING DATA FROM JSON AND LOADING TO DATABASE

def process_song_data(data, cursor):
    """
    Processes the song data and inserts into the artists and songs tables.
    Arguments:
        data: The DataFrame containing songs and artists data
        cursor: The database cursor
    """
    process_table_artists(data, cursor)
    process_table_songs(data, cursor)


# ETL PROCESSING - FILTERING, TYPECASTING, MAPPING AND MERGING DATA

def process_log_data(data, cursor):
    """
    Processes the song data and inserts it into the database.
    Arguments:
        data: The DataFrame containing time, user and songlplay data
        cursor: The database cursor
    """
    # filter by NextSong action
    data = data[data['page'] == 'NextSong']

    # convert timestamp column to datetime
    data['ts'] = pd.to_datetime(data['ts'], unit='ms')
    time_df = data['ts'].apply(
        lambda x: [x, x.hour, x.day, x.week, x.month, x.year, x.day_name()])

    # convert userId to int and generates a userId for each user with null id
    data['userId'] = data['userId'].astype('Int64')
    data['userId'] = generate_user_id(data)

    # get the song and artist IDs to build the songplays DataFrame
    ids_df = get_song_artist_ids(data, cursor)
    songplays_df = data[TABLE['songplays']].join(
        ids_df[['artist_id', 'song_id']])

    # copy time, users and songplays records to the database tables
    process_table_time(time_df, cursor)
    process_table_users(data, cursor)
    process_table_songplays(songplays_df, cursor)


# ETL JOB EXECUTOR

def process_data(dir, conn, func):
    """
    Reads multiple JSON files and executes the corresponding data processing function.
    Arguments:
        dir: The path of the JSON root directory
        conn: The database connection
        func: The function to execute for each JSON file
    """
    # get all files matching extension from directory
    all_files = get_files(dir)

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, dir))

    # get a dataframe of all files
    data = get_dataframe(all_files)
    print('{} registers found in {}'.format(data.shape[0], dir))

    func(data, conn.cursor())


# ETL JOB CONFIGURATION AND HOST SELECTION

def main(cloud):
    """
    - Selects the database host from the command line argument --cloud.
    - Create a new database session.
    - Processes all JSON files from song_data folder.
    - Processes all JSON files from log_data folder.
    - Closes the database session.

    Arguments:
        cloud: Use the cloud database connection instead of local
    """
    if cloud:
        conf = "host=heffalump.db.elephantsql.com dbname=erxsjqjd user=erxsjqjd password=OhDRrRbv8b59vECc08ENtqtG3rFekShP"
    else:
        conf = "host=127.0.0.1 dbname=sparkifydb user=student password=student"

    conn = psycopg2.connect(conf)

    process_data(dir='data/song_data', conn=conn, func=process_song_data)
    process_data(dir='data/log_data', conn=conn, func=process_log_data)

    conn.close()


# PROGRAM ENTRY POINT

if __name__ == "__main__":
    # calls the main function with the corresponding database host argument
    # getopt or argpase were not used for simplicity
    if len(sys.argv) == 2 and sys.argv[1] == '--cloud':
        main(cloud=True)
    else:
        main(cloud=False)
