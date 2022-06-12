# app libs
import streamlit as st
# style libs
from styles import *


class Sidebar:

    def __init__(self, size):
        self.size = size

    def render(self):
        with st.sidebar:
            st.header('Users')
            st.selectbox(
                'How would you like to sort users?',
                ('Most active users', 'Least active users'), key='users_option')
            st.slider('Number of users', min_value=5,
                      max_value=20, value=10, step=5, key='users_count')
            st.checkbox('Check to load all users',
                        value=False, key='users_all')
