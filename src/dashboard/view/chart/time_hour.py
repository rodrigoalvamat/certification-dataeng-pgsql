# app libs
import streamlit as st
# vizulization libs
import altair as alt
# view libs
from dashboard.view.grid.cell import GridCell


class TimeHour(GridCell):

    def __init__(self, state):
        super().__init__(state, "All songplays", "By hour")

    def render_body(self):
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
