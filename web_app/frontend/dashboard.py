import streamlit as st
import requests
from web_app.frontend.fe_utils.map_builder import build_map, build_lines
from web_app.frontend.fe_utils.ui_helpers import map_name_to_details, add_latest_weather
from web_app.config import FLASK_API_URL

st.set_page_config(layout="wide", page_title="DelayCast")

st.title("DelayCast")

try:
    if 'station_data' not in st.session_state:
        # Call Flask backend for station details
        station_res = requests.get(f"{FLASK_API_URL}/all_stations")
        #check for exceptions
        station_res.raise_for_status()
        station_data = station_res.json()

        #add weather data
        station_data = add_latest_weather(station_data)
        st.session_state['station_data'] = station_data
    else:
        station_data = st.session_state['station_data']

    if 'lines_data' not in st.session_state:
        #call flask for line details
        lines_res = requests.get(f"{FLASK_API_URL}/line_details")
        lines_res.raise_for_status()
        lines_data = lines_res.json()
        st.session_state['lines_data'] = lines_data
    else:
        lines_data = st.session_state['lines_data'] 

    #build the select box mappings
    name_details_dict = map_name_to_details(station_data)
    default_box_fill = "— Select a Station —"
    names = [default_box_fill] + sorted(name_details_dict.keys())

    #create columns
    column_one, column_two = st.columns([1, 3])

    #default map zoom
    centre = (-3.0, 54.5)  
    zoom = 5.8
    selected_code = None

    with column_one:

        st.subheader("Choose a Station")

        choice = st.selectbox("Search for a station in the list below", names, index=0, key="station_select")

        if choice and choice != default_box_fill:
            details = name_details_dict.get(choice)
            
            #extract relevant details
            selected_code = details['station_code']
            centre = (details['longitude'], details['latitude'])
            zoom = 11

            if st.button("Go to station info", type='primary', use_container_width=True):
                st.query_params['station_code'] = selected_code
                st.session_state['station_code'] = selected_code
                st.switch_page("Station_Info.py")

    with column_two:

        #build the map including station and line overlays
        st.subheader("National Map")

        lines_geojson = build_lines(lines_data)
        map = build_map(station_data, lines_geojson, centre, zoom)

        st.pydeck_chart(map, use_container_width=True)

except Exception as e:
    st.error(f'Error connecting to Flask: {e}')

