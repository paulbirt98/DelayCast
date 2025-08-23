import requests
import streamlit as st
from web_app.config import FLASK_API_URL

st.set_page_config(layout="wide", page_title="Station Info")

station_code = st.query_params.get("station_code")

if not station_code:
    station_code = st.session_state.get('station_code')
    st.query_params['station_code'] = station_code 

#home button
if st.button("Back to Home", type="primary"):
        st.switch_page("dashboard.py")

try:
    #get the rest of the station info
    station_info_res = requests.get(f'{FLASK_API_URL}/station_info', params={'station_code': station_code})
    station_info_res.raise_for_status()
    station_info = station_info_res.json()

    #get name
    station_name = station_info.get('station_name')

    st.title(f"{station_name} ({station_code})")

    #make columns
    current_condition_col, current_delay_risk_col = st.columns(3, 1)

    with current_condition_col:
        st.subheader('Current Conditions')


    
    with current_delay_risk_col:
        st.write('Current Delay Risk')

except requests.exceptions.HTTPError as e:
    st.error(f'Error fetching station info: {e}')