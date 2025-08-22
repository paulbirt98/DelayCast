import streamlit as st

#define pages
home = st.Page('dashboard.py', title="DelayCast")
station = st.Page('Station_Info.py', title="Station Info")

#define navigation
nav = st.navigation([home, station], position='hidden')
nav.run()