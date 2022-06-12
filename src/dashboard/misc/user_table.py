# app libs
import streamlit as st
# vizulization libs
import altair as alt
# style libs
from styles import *


class UserTable:

    def __init__(self, state):
        self.state = state

    def render(self):
        header = f"""
        <h5 style="{column_header_style}">{self.state.user['count']} {self.state.user['label']} active users</h5>
        <h4 style="{column_subheader_styles}">Data table</h4>
        """
        st.markdown(header, unsafe_allow_html=True)

        st.dataframe(data=self.state.user['data'], height=280)
