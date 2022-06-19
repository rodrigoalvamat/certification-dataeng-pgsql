# app libs
import streamlit as st
# vizulization libs
import altair as alt
# view libs
from dashboard.view.grid.cell import GridCell


class UserTable(GridCell):

    def __init__(self, state):
        super().__init__(state,
                         f"{state.user['count']} {state.user['label']} active users",
                         "Data table"
                         )

    def render_body(self):
        st.dataframe(data=self.state.user['data'], height=280)
