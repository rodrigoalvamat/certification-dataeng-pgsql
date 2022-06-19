# system libs
import sys
# app libs
import streamlit as st
# model libs
from dashboard.model.database import Database
# controller libs
from dashboard.controller.state import StateController
# view libs
from dashboard.view.page.home import HomePage


class App:

    def __init__(self, cloud):
        self.page = HomePage()
        self.page.set_state(StateController(Database(cloud)))

    def render(self):
        self.page.render()


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'cloud':
        app = App(cloud=True)
    else:
        app = App(cloud=False)

    app.render()
