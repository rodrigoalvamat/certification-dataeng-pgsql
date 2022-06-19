# app libs
import streamlit as st
# view libs
from dashboard.view.sidebar import Sidebar
from dashboard.view.grid.row import GridRow
from dashboard.view.chart.time_hour import TimeHour
from dashboard.view.chart.time_weekday import TimeWeekday
from dashboard.view.chart.user_gender import UserGender
from dashboard.view.chart.user_songplays import UserSongplays
from dashboard.view.chart.user_subscription import UserSubscription
from dashboard.view.map.artist_location import ArtistLocation
from dashboard.view.misc.song_word import SongWord
from dashboard.view.misc.user_table import UserTable
# style libs
from dashboard.view.styles import *


class HomePage:

    def __init__(self):
        self.state = None
        self.__init_page()
        self.__init_sidebar()

    def __init_page(self):
        st.set_page_config(
            page_title="DataDiver - Sparkify Dashboard",
            page_icon="images/favicon.ico",
            layout="wide",
            initial_sidebar_state="expanded",
            menu_items={
                'Get help': 'https://www.linkedin.com/in/rodrigo-alvarenga-mattos',
                'Report a bug': "https://github.com/rodrigoalvamat/datadiver-pgsql/issues",
                'About': "# This is a Data Enginner ETL Pipeline & Dashboard Application!"
            }
        )

    def __init_sidebar(self):
        sidebar = Sidebar(200)
        sidebar.render()

    def set_state(self, state):
        self.state = state

    def render(self):
        header = f"""
        <header style="{page_header_style}">Sparkify Dashboard</header>
        """
        st.markdown(header, unsafe_allow_html=True)

        row = GridRow([
            UserSongplays(self.state),
            UserGender(self.state),
            UserSubscription(self.state),
            ArtistLocation(self.state)
        ])
        row.render()

        row = GridRow([
            UserTable(self.state),
            TimeHour(self.state),
            TimeWeekday(self.state),
            SongWord(self.state)
        ])
        row.render()
