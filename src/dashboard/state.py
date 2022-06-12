# app libs
import streamlit as st


class StateManager:

    def __init__(self, database):
        self.__init_artist(database.artists_by_location)
        self.__init_song(database.songs_by_artist)
        self.__init_time(database.songplays_by_time)
        self.__init_user(database.songplays_by_user)

    def __init_artist(self, data):
        self.artist = {
            'data': data
        }

    def __init_song(self, data):
        self.song = {
            'data': data
        }

    def __init_time(self, data):
        self.time = {
            'data': data
        }

    def __init_user(self, data):
        if st.session_state.users_option == 'Most active users':
            users_df = data.sort_values(by='plays', ascending=False)
            users_label = 'most'
            users_sort = '-x'
        else:
            users_df = data.sort_values(by='plays', ascending=True)
            users_label = 'least'
            users_sort = 'x'

        users_count = st.session_state.users_count

        if st.session_state.users_all:
            users_count = 'All'
        else:
            users_df = users_df.head(users_count)

        self.user = {
            'count': users_count,
            'data': users_df,
            'label': users_label,
            'sort': users_sort,
        }
