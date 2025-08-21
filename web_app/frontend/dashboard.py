import streamlit as st
import requests
from web_app.frontend.utils.map_builder import build_map, build_lines
from web_app.frontend.utils.ui_helpers import map_name_to_details
from web_app.config import FLASK_API_URL

st.set_page_config(layout="wide", page_title="DelayCast")

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

            # Navigate to station page when the user clicks the button
            if st.button("Open station page", disabled=(selected_code is None)):
                if selected_code:
                    st.query_params["code"] = selected_code   # keep consistent with Station page
                    try:
                        st.switch_page("web_app/frontend/pages/Station.py")
                    except Exception:
                        st.rerun()

    with column_two:

        #build the map including station and line overlays
        st.subheader("National Map")

        lines_geojson = build_lines(lines_data)
        map = build_map(station_data, lines_geojson, centre, zoom)

        st.pydeck_chart(map, use_container_width=True)

except Exception as e:
    st.error(f'Error connecting to Flask: {e}')

