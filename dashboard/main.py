# data app libs
import streamlit as st

# etl/eda libs
import pandas as pd

# sql libs
from sqlalchemy import create_engine


database = 'postgresql+psycopg2://student:student@127.0.0.1/sparkifydb'
#database = 'postgresql+psycopg2://erxsjqjd:OhDRrRbv8b59vECc08ENtqtG3rFekShP@heffalump.db.elephantsql.com/erxsjqjd'


engine = create_engine(database)

query = """
SELECT COUNT(*) AS plays, u.first_name AS user
FROM songplays AS s
JOIN users AS u
ON s.user_id = u.user_id
GROUP BY s.user_id, u.first_name
ORDER BY plays DESC;
"""

songplays = pd.read_sql(query, engine)

st.write("""
# Sparkify Dashboard
""")

st.bar_chart(songplays.head(20))