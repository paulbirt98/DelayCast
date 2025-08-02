import streamlit as st
import requests
import os

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
except Exception as e:
    st.error(f'Error connecting to Flask: {e}')

