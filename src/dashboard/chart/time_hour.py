# app libs
import streamlit as st
# vizulization libs
import altair as alt
# style libs
from styles import *


class TimeHour:

    def __init__(self, state):
        self.state = state

    def render(self):
        header = f"""
        <h5 style="{column_header_style}">All songplays</h5>
        <h4 style="{column_subheader_styles}">By hour</h4>
        """
        st.markdown(header, unsafe_allow_html=True)

        data = self.state.time['data']
        data = data.groupby(by=['hour'], as_index=False).aggregate(
            {'timestamp': 'count'})

        graph = alt.Chart(data).mark_bar().encode(
            x=alt.X('hour:Q'),
            y=alt.Y('timestamp:Q', title='plays'),
            color=alt.Color('hour:Q', sort='x', legend=None,
                            scale=alt.Scale(scheme='tealblues'))
        ).properties(
            width='container',
            height=300
        ).interactive()

        st.altair_chart(graph, use_container_width=True)
