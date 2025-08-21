import streamlit as st
import requests
from web_app.config import FLASK_API_URL

st.set_page_config(layout="wide", page_title="Station")

code = st.query_params.get("code")
if not code:
    st.error("No station code provided.")
    st.stop()

st.title(f"Station: {code}")

col1, _ = st.columns([1, 6])
with col1:
    if st.button("← Back to National Map"):
        st.query_params.clear()
        st.switch_page("web_app/frontend/dashboard.py")