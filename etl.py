# system libs
import os
import glob

# etl/eda libs
import pandas as pd

# sql libs
import psycopg2
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a JSON song file and executes the insert statements for songs and artists tables.
    Arguments:
        cur: The database connection cursor
        filepath: The path of the JSON file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[['song_id', 'title', 'year', 'duration', 'artist_id']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    

def process_log_file(cur, filepath):
    """
    Processes a JSON log file and executes the insert statements for time, users and songplays tables.
    Arguments:
        cur: The database connection cursor
        filepath: The path of the JSON file
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

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    user_df['userId'] = user_df['userId'].astype('int64')

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.level, row.location, row.userAgent, artistid, row.sessionId, songid, row.ts, row.userId]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(conn, filepath, func):
    """
    Reads multiple JSON files and executes the corresponding data processing function.
    Arguments:
        conn: The database connection
        filepath: The path of the JSON folder root
        func: The function to execute for each JSON file
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
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Create a new database session.
    - Processes all song JSON files data.
    - Processes all log JSON files data.
    - Closes the database session.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")

    process_data(conn, filepath='data/song_data', func=process_song_file)
    process_data(conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()