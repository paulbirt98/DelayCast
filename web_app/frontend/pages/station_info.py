import streamlit as st
from web_app.config import FLASK_API_URL

st.set_page_config(layout="wide", page_title="Station Info")

station_code = st.query_params.get("code")

st.title(f"Station: {station_code}")
st.write(f"You are now viewing data for station: **{station_code}**")