# system libs
import os
# data libs
import pandas as pd
# sql libs
from sqlalchemy import create_engine
# sql queries
from queries import *


class Database:
    def __init__(self, cloud):
        if cloud:
            # cloud database connection configuration
            host = os.environ['PGSQL_CLOUD_HOST']
            username = os.environ['PGSQL_CLOUD_USERNAME']
            password = os.environ['PGSQL_CLOUD_PASSWORD']
            conf = f'postgresql+psycopg2://{username}:{password}@{host}/{username}'
        else:
            conf = 'postgresql+psycopg2://student:student@127.0.0.1/sparkifydb'

        connection = create_engine(conf)

        self.artists_by_location = pd.read_sql(artists_by_location, connection)
        self.songplays_by_time = pd.read_sql(songplays_by_time, connection)
        self.songplays_by_user = pd.read_sql(songplays_by_user, connection)
        self.songs_by_artist = pd.read_sql(songs_by_artist, connection)

        connection.dispose()
