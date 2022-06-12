# system libs
import sys
# app libs
import streamlit as st
# middleware libs
from database import Database
from state import StateManager
# component libs
from sidebar import Sidebar
from chart.time_hour import TimeHour
from chart.time_weekday import TimeWeekday
from chart.user_gender import UserGender
from chart.user_songplays import UserSongplays
from chart.user_subscription import UserSubscription
from map.artist_location import ArtistLocation
from misc.song_word import SongWord
from misc.user_table import UserTable
# style libs
from styles import *


class App:

    def __init__(self, cloud):
        self.__config_page()
        self.__config_sidebar()
        self.state = StateManager(Database(cloud))

    def __config_page(self):
        st.set_page_config(
            page_title="DataDiver - Sparkify Dashboard",
            page_icon="images/favicon.ico",
            layout="wide",
            initial_sidebar_state="expanded",
            menu_items={
                'Get help': 'https://www.linkedin.com/in/rodrigo-alvarenga-mattos',
                'Report a bug': "https://github.com/rodrigoalvamat/portfolio/issues",
                'About': "# This is a Data Enginner ETL Pipeline showcase app!"
            }
        )
    
    def __config_sidebar(self):
        sidebar = Sidebar(200)
        sidebar.render()

    def __render_top_row(self):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            songplays = UserSongplays(self.state)
            songplays.render()

        with col2:
            gender = UserGender(self.state)
            gender.render()

        with col3:
            subscription = UserSubscription(self.state)
            subscription.render()

        with col4:
            location = ArtistLocation(self.state)
            location.render()

    def __render_bottom_row(self):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            table = UserTable(self.state)
            table.render()

        with col2:
            hour = TimeHour(self.state)
            hour.render()

        with col3:
            weekday = TimeWeekday(self.state)
            weekday.render()

        with col4:
            wordcloud = SongWord(self.state)
            wordcloud.render()

    def render(self):
        header = f"""
        <header style="{page_header_style}">Sparkify Dashboard</header>
        """
        st.markdown(header, unsafe_allow_html=True)

        self.__render_top_row()
        self.__render_bottom_row()


if __name__ == "__main__":

    if len(sys.argv) == 2 and sys.argv[1] == 'cloud':
        app = App(cloud=True)
    else:
        app = App(cloud=False)

    app.render()
