import requests
import streamlit as st
from web_app.frontend.fe_utils.ui_helpers import determine_icon, determine_description
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

    #get station weather
    forecast_res = requests.get(f'{FLASK_API_URL}/location_forecast', params={'station_code': station_code})
    forecast_res.raise_for_status()
    forecast_data = forecast_res.json()

    #get current conditions
    current_conditions = forecast_data['hourly_forecasts'][0]

    #icon + description
    weather_code = current_conditions['weather_code']
    is_day = current_conditions['is_day']
    gusts = current_conditions['gusts']
    icon = determine_icon(weather_code, is_day, gusts)
    description = determine_description(weather_code, gusts)

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

            #get weather conditions
            temp = round(float(current_conditions['temp_2m']))
            humidity = round(float(current_conditions['humidity']))
            rain = round(current_conditions['rain'], 2)
            pressure = round(current_conditions['surface_pressure'])
            snow_depth_cm = round((current_conditions['snow_depth'] * 100), 1)

            st.markdown(
                f"""
                <div style="
                    font-size: 80px;
                    font-weight: bold;
                    text-align: center;
                    padding: 20px;
                    border-radius: 15px;
                ">
                    {temp}°C
                </div>
                """,
                unsafe_allow_html=True
            )

            st.markdown(
                f"""
                <div style="text-align:center; line-height:1.7; padding-bottom:20px">
                    <div>Humidity: <b>{humidity}%</b></div>
                    <div>Rain (last hour): <b>{rain:.2f} mm</b></div>
                    <div>Surface Pressure: <b>{pressure} hPa</b></div>
                    <div>Snow Depth: <b>{snow_depth_cm} cm</b></div>
                </div>
                """,
                unsafe_allow_html=True
            )
    
    with current_delay_risk_col:
        st.markdown(
            "<h2 style='text-align: center; font-weight: 600;'>Current Delay Risk (>5mins)</h3>",
            unsafe_allow_html=True
        )

        try:
            resp = requests.get(
                f"{FLASK_API_URL}/delay_risk_glq_inv",
                params={"station_code": station_code}
            )
            resp.raise_for_status()
            risk_forecast = resp.json()
            hourly_risk = risk_forecast.get('hourly_risk', [])
        except Exception:
            print('Error: Problem fetching risk forecast from Flask')
            hourly_risk = []

        #get current risk to dispaly
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

except requests.exceptions.HTTPError as e:
    st.error(f'Error fetching station info: {e}')