# app libs
import streamlit as st


class GridRow():

    def __init__(self, cells):
        self.cells = cells

    def render(self):
        columns = st.columns(len(self.cells))
        for index, cell in enumerate(self.cells):
            with columns[index]:
                cell.render()
