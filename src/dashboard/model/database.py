# system libs
import os
# data libs
import pandas as pd
# sql libs
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
# sql queries
from dashboard.model.queries import *
# etl libs
from etl.create_tables import main as create_tables
from etl.etl2 import main as etl2


class Database:
    def __init__(self, cloud):
        config = self.__create_config(cloud)
        self.__init_dataframes(config)

    def __create_config(self, cloud):
        if cloud:
            # cloud database connection configuration
            host = os.environ['PGSQL_CLOUD_HOST']
            username = os.environ['PGSQL_CLOUD_USERNAME']
            password = os.environ['PGSQL_CLOUD_PASSWORD']
            config = f'postgresql+psycopg2://{username}:{password}@{host}/{username}'
        else:
            config = 'postgresql+psycopg2://student:student@127.0.0.1/sparkifydb'

        return config

    def __create_connection(self, config):
        connection = create_engine(config)
        try:
            connection.connect()
        except OperationalError:
            create_tables(cloud=False)
            etl2(cloud=False)
            connection.connect()
            
        return connection

    def __init_dataframes(self, config):
        connection = self.__create_connection(config)

        self.artists_by_location = pd.read_sql(artists_by_location, connection)
        self.songplays_by_time = pd.read_sql(songplays_by_time, connection)
        self.songplays_by_user = pd.read_sql(songplays_by_user, connection)
        self.songs_by_artist = pd.read_sql(songs_by_artist, connection)

        connection.dispose()
