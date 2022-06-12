# app libs
import streamlit as st
# vizulization libs
import altair as alt
# style libs
from styles import *


class UserGender:

    def __init__(self, state):
        self.state = state

    def render(self):
        header = f"""
        <h5 style="{column_header_style}">{self.state.user['count']} {self.state.user['label']} active users</h5>
        <h4 style="{column_subheader_styles}">By gender</h4>
        """
        st.markdown(header, unsafe_allow_html=True)

        graph = alt.Chart(self.state.user['data']).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field='gender', type='nominal', aggregate='count'),
            color=alt.Color(field='gender', type='nominal',
                            scale=alt.Scale(scheme='blues')),
        ).properties(
            width='container',
            height=300
        ).interactive()

        st.altair_chart(graph, use_container_width=True)
