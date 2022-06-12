# app libs
import streamlit as st
# vizulization libs
import matplotlib.pyplot as plt
from wordcloud import WordCloud
# style libs
from styles import *


class SongWord:

    def __init__(self, state):
        self.state = state

    def render(self):
        header = f"""
        <h5 style="{column_header_style}">All song titles</h5>
        <h4 style="{column_subheader_styles}">Word cloud</h4>
        """
        st.markdown(header, unsafe_allow_html=True)

        text = ' '.join(song for song in self.state.song['data']['title'])
        text = text.replace('Album', '').replace('Version', '')
        wordcloud = WordCloud(background_color='white',
                              width=380, height=280).generate(text)

        fig = plt.figure(figsize=(9, 9))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()
        st.pyplot(fig)
