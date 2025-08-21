import streamlit as st
import requests
from utils.map_builders import build_map, build_lines
from web_app.config import FLASK_API_URL


st.title("DelayCast")


try:
    # Call Flask backend for station details
    station_res = requests.get(f"{FLASK_API_URL}/station_details")
    #check for exceptions
    station_res.raise_for_status()
    station_data = station_res.json()

    #call flask for line details
    lines_res = requests.get(f"{FLASK_API_URL}/line_coords")
    lines_res.raise_for_status()
    lines_data = lines_res.json()

    #build the map including station and line overlays
    st.subheader("National Map")

    lines_geojson = build_lines(lines_data)
    map = build_map(station_data, lines_geojson)

    st.pydeck_chart(map, use_container_width=True)

except Exception as e:
    st.error(f'Error connecting to Flask: {e}')

