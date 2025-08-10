import json
import streamlit as st
from streamlit.components.v1 import html
import requests
import os
import folium
import pandas as pd
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]  # .../DelayCast
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from pipeline.pipeline_utils.config import METADATA
import geopandas as gpd
from shapely.geometry import LineString

st.title("DelayCast")

#deployment url or localhost port no. as default during development
FLASK_API_URL = os.getenv("FLASK_API_URL", "http://127.0.0.1:5000")

try:
    # Call Flask backend
    res = requests.get(f"{FLASK_API_URL}/api/message")

    #check for exceptions
    res.raise_for_status()

    data = res.json()

    st.success(f"Flask is connected: {data['message']}")

    st.subheader("National Map")
    m = folium.Map(location=[54.5, -3], zoom_start=6)
    #render map
    folium.TileLayer(
        tiles="https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png",
        attr='&copy; <a href="https://www.stadiamaps.com/">Stadia Maps</a> '
             '&copy; <a href="https://openmaptiles.org/">OpenMapTiles</a> '
             '&copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors',
        name="Stadia.AlidadeSmooth",
        max_zoom=20
    ).add_to(m)
    

    all_stations = {}

    #get station coords from each route
    for file in METADATA.iterdir():
        if file.suffix.lower() == '.json':
            with open(file, "r", encoding="utf-8") as f: #read the json file
                data = json.load(f)

                for station, coords in data.items():
                    latitude = coords.get('latitude')
                    longitude = coords.get('longitude')
                    if latitude is None or longitude is None:
                        continue
                    if station not in all_stations:
                        all_stations[station] = (latitude, longitude) 

    #render all stations to map
    for station, (latitude, longitude) in all_stations.items():
        folium.CircleMarker(
            location=[latitude, longitude],
            radius=3,
            popup=f'{station}',
            color='#000000',
            fill=True, fill_opacity=0.8, 
        ).add_to(m)

    #get route line visuals
    core = pd.read_csv(METADATA / 'nf_core.csv')

    geo_data = []

    for elr, group in core.groupby('elr'):
        group_sorted = group.sort_values('total_yards')
        coords = list(zip(group_sorted['longitude'], group_sorted['latitude']))
        geo_data.append({'elr': elr, 'geometry': LineString(coords)})

    #get geo dataframe
    geo_df = gpd.GeoDataFrame(geo_data, crs='EPSG:4326')

    geo_df.to_file('studied_routes.geojson', driver="GeoJSON")

    html(m._repr_html_(), height=500)

except Exception as e:
    st.error(f'Error connecting to Flask: {e}')

