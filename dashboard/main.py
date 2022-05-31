# data app libs
import streamlit as st

# etl/eda libs
import pandas as pd

# sql libs
import psycopg2
from sqlalchemy import create_engine


engine = create_engine('postgresql+psycopg2://student:student@127.0.0.1/sparkifydb')

query = 'SELECT * FROM songplays'

songplays = pd.read_sql(query, engine)

songplays