# app libs
import streamlit as st
# vizulization libs
import altair as alt
# view libs
from dashboard.view.grid.cell import GridCell


class UserSubscription(GridCell):

    def __init__(self, state):
        super().__init__(state,
                         f"{state.user['count']} {state.user['label']} active users",
                         "By songs played"
                         )

    def render_body(self):
        graph = alt.Chart(self.state.user['data']).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field='gender', type='nominal', aggregate='count'),
            color=alt.Color(field='gender', type='nominal',
                            scale=alt.Scale(scheme='blues')),
        ).properties(
            width='container',
            height=300
        ).interactive()

        st.altair_chart(graph, use_container_width=True)
