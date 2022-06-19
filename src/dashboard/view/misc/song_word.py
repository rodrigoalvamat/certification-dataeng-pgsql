# app libs
import streamlit as st
# vizulization libs
import matplotlib.pyplot as plt
from wordcloud import WordCloud
# view libs
from dashboard.view.grid.cell import GridCell


class SongWord(GridCell):

    def __init__(self, state):
        super().__init__(state, "All song titles", "Word cloud")

    def render_body(self):
        text = ' '.join(song for song in self.state.song['data']['title'])
        text = text.replace('Album', '').replace('Version', '')
        wordcloud = WordCloud(background_color='white',
                              width=380, height=280).generate(text)

        fig = plt.figure(figsize=(9, 9))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()
        st.pyplot(fig)
