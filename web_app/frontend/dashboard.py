import streamlit as st
import requests
from utils.map_builders import build_base_map, add_stations, add_lines
from streamlit.components.v1 import html
from web_app.config import FLASK_API_URL

st.title("DelayCast")


try:
    # Call Flask backend for station details
    station_res = requests.get(f"{FLASK_API_URL}/station_details")
    #check for exceptions
    station_res.raise_for_status()

    station_data = station_res.json()

    st.subheader("National Map")
    m = build_base_map()
    add_stations(m, station_data)

    html(m._repr_html_(), height=500)
    

except Exception as e:
    st.error(f'Error connecting to Flask: {e}')

