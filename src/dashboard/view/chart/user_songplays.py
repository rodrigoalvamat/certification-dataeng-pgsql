# app libs
import streamlit as st
# vizulization libs
import altair as alt
# view libs
from dashboard.view.grid.cell import GridCell


class UserSongplays(GridCell):

    def __init__(self, state):
        super().__init__(state,
                         f"{state.user['count']} {state.user['label']} active users",
                         "By songs played"
                         )

    def render_body(self):
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
