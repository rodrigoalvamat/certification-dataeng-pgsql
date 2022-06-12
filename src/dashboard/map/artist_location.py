# app libs
import streamlit as st
# vizulization libs
import pydeck as pdk
# style libs
from styles import *


class ArtistLocation:

    def __init__(self, state):
        self.state = state

    def render(self):
        header = f"""
        <h5 style="{column_header_style}">All artists</h5>
        <h4 style="{column_subheader_styles}">Location coordinates</h4>
        """
        st.markdown(header, unsafe_allow_html=True)

        data = self.state.artist['data']
        data = data.loc[data['latitude'].notnull() &
                        data['longitude'].notnull()]

        latitude = data['latitude'].mean()
        longitude = data['longitude'].mean()

        st.pydeck_chart(pdk.Deck(
            map_style='mapbox://styles/mapbox/light-v9',
            initial_view_state=pdk.ViewState(
                latitude=latitude,
                longitude=longitude,
                zoom=1,
                height=280,
                width=370
            ),
            layers=[
                pdk.Layer(
                    'ScatterplotLayer',
                    data=data,
                    get_position=['longitude', 'latitude'],
                    get_color='[30, 150, 200, 160]',
                    radius_scale=2,
                    radius_min_pixels=8,
                    radius_max_pixels=128,
                    line_width_min_pixels=1,
                    get_radius="exits_radius",
                    auto_highlight=True
                ),
            ],
        ))
