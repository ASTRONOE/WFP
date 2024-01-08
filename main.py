import streamlit as st
import pandas as pd
import os
import countryapi
from deta import Deta
from dotenv import load_dotenv
from PIL import Image
from streamlit_autorefresh import st_autorefresh

st_autofresh(interval=7000, key='isrunning')
def main():
  #Set page config
  st.set_page_config(
    page_title="Global Food Price Tracker and Explorer",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon=Image.open("assets/fruitbasket.png")
  )
  
  with st.container():
    st.title("Global Food Price Tracker and Explorer")
    st.markdown('''---''')
    st.header("Exploring Worldwide Crop Prices: Tracking Trends And Variations Across Different Regions", divider='orange')
    st.markdown('''---''')
#Run  
if __name__ == '__main__':
  main()