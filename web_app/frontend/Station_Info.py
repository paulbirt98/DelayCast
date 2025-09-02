from email.utils import parsedate_to_datetime
import pandas as pd
import streamlit as st
from web_app.frontend.fe_utils.ui_helpers import station_weather_risk_forecast, determine_weather_label, determine_time_graphs
from datetime import datetime
from web_app.config import TABLE_CSS, GRAPH_CSS, LOADER_HTML

st.set_page_config(layout="wide", page_title="Station Info")

#pull station code from the url if given
station_code = st.query_params.get("station_code")

#let station code persist if refreshed
if not station_code:
    station_code = st.session_state.get('station_code')
    st.query_params['station_code'] = station_code

needs_fetch = ("data" not in st.session_state) or (st.session_state.get("data_station_code") != station_code)

loader_slot = st.empty()
if needs_fetch:
    # show full-screen loader before fetchign forecast data
    loader_slot.markdown(LOADER_HTML, unsafe_allow_html=True)

#catch cases where station is still none and send home
try:
    if needs_fetch:
        all_data = station_weather_risk_forecast(station_code)
        st.session_state["data"] = all_data
        st.session_state["data_station_code"] = station_code
    else:
        all_data = st.session_state["data"]
except Exception as e:
    loader_slot.empty()
    st.error("Oops! Something went wrong fetching station data. Please return to the home page.")
    if st.button("Back to Home", type="primary"):
        st.switch_page("dashboard.py")
    st.stop()
finally:
    loader_slot.empty()

#home button
if st.button("Back to Home", type="primary"):
        st.switch_page("dashboard.py")


#get the rest of the station info
station_info = all_data.get('station_info', {})

#get name
station_name = station_info.get('station_name')

st.title(f"{station_name} ({station_code})")

#make columns
current_condition_col, current_delay_risk_col = st.columns([2, 1])

with current_condition_col:
    st.markdown(
        "<h2 style='text-align: center; font-weight: 600;'>Current Conditions</h3>",
        unsafe_allow_html=True
    )

    #make an icon and a temp column
    icon_col, temp_col = st.columns([1, 1])

    with icon_col:

        #icon + description
        icon = all_data.get('icon')
        description = all_data.get('description')
        
        st.markdown(
            f"""
            <div style="text-align:center;">
                <img src="{icon}" height="200" width="200">
                <p><b>{description}</b></p>
            </div>
            """,
            unsafe_allow_html=True
        )

    with temp_col:

        #get current conditions
        current_conditions = all_data.get('current_conditions', {})

        #get current weather variables
        current_temp = round(float(current_conditions['temp_2m']))
        current_humidity = round(float(current_conditions['humidity']))
        current_rain = round(current_conditions['rain'], 2)
        current_gusts = round(current_conditions['gusts'])
        current_pressure = round(current_conditions['surface_pressure'])
        current_snow_depth_cm = round((current_conditions['snow_depth'] * 100), 1)

        st.markdown(
            f"""
            <div style="
                font-size: 80px;
                font-weight: bold;
                text-align: center;
                padding: 20px;
                border-radius: 15px;
            ">
                {current_temp}°C
            </div>
            """,
            unsafe_allow_html=True
        )

        st.markdown(
            f"""
            <div style="text-align:center; line-height:1.7; padding-bottom:20px">
                <div>Humidity: <b>{current_humidity}%</b></div>
                <div>Rain (last hour): <b>{current_rain:.2f} mm</b></div>
                <div>Gusts: <b>{current_gusts} km/h</b></div>
                <div>Surface Pressure: <b>{current_pressure} hPa</b></div>
                <div>Snow Depth: <b>{current_snow_depth_cm} cm</b></div>
            </div>
            """,
            unsafe_allow_html=True
        )

with current_delay_risk_col:
    st.markdown(
        "<h2 style='text-align: center; font-weight: 600;'>Current Delay Risk (>5mins)</h3>",
        unsafe_allow_html=True
    )

    hourly_risk = all_data.get('hourly_risk', {})

    #get current risk to dispaly
    current_risk = hourly_risk[0]['probs']

    if not hourly_risk:
        st.warning("No current delay risk data available for this station right now.")
        current_risk = {}
    else:
        current_risk = hourly_risk[0]['probs']

    # fixed display order
    classes = ["No Delay", "Minor Delay", "Moderate Delay", "Severe Delay"]

    current_overall = float(
        current_risk.get(classes[1], 0.0) + 
        current_risk.get(classes[2], 0.0) + 
        current_risk.get(classes[3], 0.0)
        ) * 100

    #dispaly overall delay risk as headline
    st.markdown(
            f"""
            <div style="
                font-size: 80px;
                font-weight: bold;
                text-align: center;
                padding: 20px;
                border-radius: 15px;
            ">
                {f"{current_overall:.0f}%"}
            </div>
            """,
            unsafe_allow_html=True
        )
    
    #disaply subclasses below
    current_moderate = float(current_risk.get(classes[2], 0.0)) * 100
    current_severe = float(current_risk.get(classes[3], 0.0)) * 100

    st.markdown(
            f"""
            <div style="text-align:center; line-height:1.7; padding-bottom:20px">
                <div>Moderate Delay (>15 mins): <b>{f"{current_moderate:.0f}%"}</b></div>
                <div>Severe Delay (>30 mins): <b>{f"{current_severe:.0f}%"}</b></div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    # weather impacts on the current hour delay risk
    current_weather_influences = (all_data.get('hourly_risk', []) or [])
    current_weather_influences = current_weather_influences[0].get("top_features", []) if current_weather_influences else []

    # get only non zero and up to 3
    current_weather_influences = [influence for influence in current_weather_influences if abs(influence.get('pp')) != 0][:3]

    if current_weather_influences:
        influences_pps = " • ".join(f"{determine_weather_label(influence['feature'])} {influence['pp']:+.0f}pp" for influence in current_weather_influences)
        st.markdown(
            f"""
            <div style="text-align:center; line-height:1.7; padding-bottom:20px">
                <span style="font-weight:600;">Weather Influence:</span><br/>
                <span>{influences_pps}</span>
            </div>
            """,
            unsafe_allow_html=True
        )

    
#inject styling for the tab layout to centre tabs and space tem out
st.markdown(TABLE_CSS, unsafe_allow_html=True)

#delay risk forecast for next 5 days display
forecast_by_day = all_data.get("forecast_by_day", {}) 
days = all_data.get("days", []) 

st.markdown("<h3 style='text-align:center;'>5 Day Forecast</h3>", unsafe_allow_html=True)

#create tabs for each day in the forecast
tabs = st.tabs([datetime.fromisoformat(day).strftime("%a %d %b") for day in days])

#loop through each tab and day
for tab, day in zip(tabs, days):

    #get the hoourly forecast for that day
    with tab:
        hours_data = forecast_by_day[day]

        #take care of missing days
        if not hours_data:
            st.info("No forecast data available for this day.")
            continue

        # Build column headers (hours)
        hour_labels = [h["timestamp_utc"].strftime("%H:%M") for h in hours_data]

        #get table structure
        table = {
            "": [
                "Delay Risk (>5mins)",
                "Weather Influence #1",
                "Weather Influence #2",
                "Weather Influence #3",
                "Moderate Delay(>15mins)",
                "Severe Delay(>30mins)",
                "Temperature",
                "Humidity",
                "Rain (Previous Hour - mm)",
                "Gusts (km/h)",
                "Surface Pressure (hPa)",
                "Snow Depth",
            ]
        }

        # get lsits for each row
        delay_row = []
        influence_row_1 =[]
        influence_row_2 =[]
        influence_row_3 =[]
        moderate_row = []
        severe_row = []
        temp_row = []
        humidity_row = []
        rain_row = []
        gusts_row=[]
        pressure_row = []
        snow_row = []

        #go through each hourly forecast and append values
        for hour in hours_data:
            probs = hour["probs"]
            weather = hour["weather"]

            delay_row.append(f'{(probs.get("Minor Delay", 0) + probs.get("Moderate Delay", 0) + probs.get("Severe Delay", 0)) * 100:.0f}%')
            moderate_row.append(f'{probs.get("Moderate Delay", 0) * 100:.0f}%')
            severe_row.append(f'{probs.get("Severe Delay", 0) * 100:.0f}%')
            temp_row.append(f'{round(weather.get("temp_2m", 0))}')
            humidity_row.append(f'{round(weather.get("relative_humidity", 0))}')
            rain_row.append(f'{weather.get("rain", 0):.2f}')
            gusts_row.append(f'{(round(weather.get("gusts", 0)))}')
            pressure_row.append(f'{round(weather.get("surface_pressure", 0))}')
            snow_row.append(f'{round(weather.get("snow_depth")*100, 1)}')

            #add weather influence if applicable
            influences = hour.get("top_features") or []
            influences = [influence for influence in influences if abs(influence.get("pp", 0)) != 0][:3]

            influence_count = len(influences)

            # format helper
            def format_influence(influence):
                name = determine_weather_label(influence.get("feature")) or str(influence.get("feature")).replace("_", " ").title()
                return f"{name} {influence.get('pp', 0):+0.0f}pp"

            # fill 3 slots
            influence_row_1.append(format_influence(influences[0]) if len(influences) >= 1 else "N/A")
            influence_row_2.append(format_influence(influences[1]) if len(influences) >= 2 else "N/A")
            influence_row_3.append(format_influence(influences[2]) if len(influences) >= 3 else "N/A")
    
        # Add hour columns with values 
        for index, hour in enumerate(hour_labels):
            table[hour] = [
                delay_row[index],
                influence_row_1[index],
                influence_row_2[index],
                influence_row_3[index],
                moderate_row[index],
                severe_row[index],
                temp_row[index],
                humidity_row[index],
                rain_row[index],
                gusts_row[index],
                pressure_row[index],
                snow_row[index]
            ]

        #config column width to avoid cut offs
        col_cfg = {h: st.column_config.TextColumn(h, width=0) for h in hour_labels}

        #table to dataframe
        df = pd.DataFrame(table)

        # data_editor but read - only (to allowtwext wrappignw)
        st.data_editor(
            df,
            hide_index=True,
            width='stretch',
            disabled=True,  
            column_config={
                col: st.column_config.TextColumn(
                    col,
                    width='medium'
                ) for col in df.columns
            }
        )

#time graphs
st.markdown(GRAPH_CSS, unsafe_allow_html=True)

graphs = determine_time_graphs(station_code)

if graphs["hour"]:
    st.markdown(
        "<h2 style='text-align: center; font-weight: 600;'>Average Delays by Hour of the Day</h3>",
        unsafe_allow_html=True
    )
    st.markdown(f"<div class='graph-wrap'>{graphs['hour']}</div>", unsafe_allow_html=True)


if graphs["day"]:
    st.markdown(
        "<h2 style='text-align: center; font-weight: 600;'>Average Delays by Day of the Week</h3>",
        unsafe_allow_html=True
    )
    st.markdown(f"<div class='graph-wrap'>{graphs['day']}</div>", unsafe_allow_html=True)

if graphs["month"]:
    st.markdown(
        "<h2 style='text-align: center; font-weight: 600;'>Average Delays by Month of the Year</h3>",
        unsafe_allow_html=True
    )
    st.markdown(f"<div class='graph-wrap'>{graphs['month']}</div>", unsafe_allow_html=True)

