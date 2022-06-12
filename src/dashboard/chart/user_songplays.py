# app libs
import streamlit as st
# vizulization libs
import altair as alt
# style libs
from styles import *


class UserSongplays:

    def __init__(self, state):
        self.state = state

    def render(self):
        header = f"""
        <h5 style="{column_header_style}">{self.state.user['count']} {self.state.user['label']} active users</h5>
        <h4 style="{column_subheader_styles}">By songs played</h4>
        """
        st.markdown(header, unsafe_allow_html=True)

        graph = alt.Chart(self.state.user['data']).mark_bar().encode(
            x='plays',
            y=alt.Y('user:N', sort=self.state.user['sort']),
            color=alt.Color('user:N', sort=self.state.user['sort'], legend=None,
                            scale=alt.Scale(scheme='teals'))
        ).properties(
            width='container',
            height=300
        ).interactive()

        st.altair_chart(graph, use_container_width=True)
