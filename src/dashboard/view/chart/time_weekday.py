# app libs
import streamlit as st
# vizulization libs
import altair as alt
# view libs
from dashboard.view.grid.cell import GridCell


class TimeWeekday(GridCell):

    def __init__(self, state):
        super().__init__(state, "All songplays", "By weekdays")

    def render_body(self):
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
