# app libs
import streamlit as st
# vizulization libs
import altair as alt
# style libs
from styles import *


class TimeWeekday:

    def __init__(self, state):
        self.state = state

    def render(self):
        header = f"""
        <h5 style="{column_header_style}">All songplays</h5>
        <h4 style="{column_subheader_styles}">By weekdays</h4>
        """
        st.markdown(header, unsafe_allow_html=True)

        data = self.state.time['data']
        data = data.groupby(by=['weekday'], as_index=False).aggregate(
            {'timestamp': 'count'})

        weekdays = ['Monday', 'Tuesday', 'Wednesday',
                    'Thursday', 'Friday', 'Saturday', 'Sunday']

        graph = alt.Chart(data).mark_bar().encode(
            x=alt.X('weekday:N', sort=weekdays),
            y=alt.Y('timestamp:Q', title='plays'),
            color=alt.Color('weekday:N', sort=weekdays, legend=None,
                            scale=alt.Scale(scheme='teals'))
        ).properties(
            width='container',
            height=300
        ).interactive()

        st.altair_chart(graph, use_container_width=True)
