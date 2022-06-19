# abstract classs
from abc import ABC, abstractmethod
# app libs
import streamlit as st
# style libs
from dashboard.view.styles import *


class GridCell(ABC):

    def __init__(self, state, header, subheader):
        self.state = state
        self.header = header
        self.subheader = subheader

    def __render_header(self):
        header = f"""
        <h5 style="{column_header_style}">{self.header}</h5>
        <h4 style="{column_subheader_styles}">{self.subheader}</h4>
        """
        st.markdown(header, unsafe_allow_html=True)

    @abstractmethod
    def render_body(self):
        pass

    def render(self):
        self.__render_header()
        self.render_body()
