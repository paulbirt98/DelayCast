from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
import requests
from web_app.config import FLASK_API_URL,WINDY, get_route_code
from web_app.frontend.icon_logic import WEATHER_ICON_MAP, WEATHER_ICON_DIR, WIND, WEATHER_DESCRIPTION_MAP
import base64
import streamlit as st

def map_name_to_details(station_data):
    """
    
    """
    name_to_code = {}

    for station in station_data:
        name = station.get("station_name")
        code = station.get("station_code")
        longitude = station.get("longitude")
        latitude = station.get("latitude")

        #avoid dupes
        if (name and code) and name not in name_to_code:
            name_to_code[name] = {'station_code': code, 'longitude': float(longitude),'latitude': float(latitude)}

    return name_to_code

def determine_icon(weather_code, is_day, gusts):
    """
    
    """
    if gusts > WINDY and weather_code <=3:
        icon_filepath = WEATHER_ICON_DIR / WIND
    elif is_day:
        icon_filepath = WEATHER_ICON_DIR / WEATHER_ICON_MAP.get(weather_code)['day']
    else:
        icon_filepath = WEATHER_ICON_DIR / WEATHER_ICON_MAP.get(weather_code)['night']
    
    icon_bytes = (icon_filepath).read_bytes()
    icon = "data:image/svg+xml;base64," + base64.b64encode(icon_bytes).decode("ascii")

    return icon

def determine_description(weather_code, gusts):
    """
    
    """
    if gusts > WINDY and weather_code <=3:
        description = 'Windy'
    else:
        description = WEATHER_DESCRIPTION_MAP.get(weather_code)
    
    return description

def add_latest_weather(station_data):
    """
    
    """

    for station in station_data:
        try:
            res = requests.get(f'{FLASK_API_URL}/location_forecast', params={'station_code': station['station_code']})
            res.raise_for_status()
            
            forecast = res.json()

            #get most recent
            current_hour = forecast['hourly_forecasts'][0]
            station['timestamp'] = current_hour['timestamp']

            weather_code = int(current_hour['weather_code'])
            is_day = bool(current_hour['is_day'])
            gusts = float(current_hour['gusts'])

            station['weather_code'] = weather_code
            station['is_day'] = is_day
            station['gusts'] = gusts

            #get icon
            station['icon'] = determine_icon(weather_code, is_day, gusts)

        except:
            pass

    return station_data

def station_weather_risk_forecast(station_code):
    """
    Fetches all data to be used on the statino info page - the hourly weather and hourly risk forecasts for the next 5 days, 6am to 11pm.

    Args:
    - station_code (str): the three letter station code for which data is to be fetched and computed

    Returns:
    - 
    """
    #get station info from flask
    try:
        station_info_res = requests.get(f"{FLASK_API_URL}/station_info", params={"station_code": station_code})
        station_info_res.raise_for_status()
        station_info = station_info_res.json()
    except Exception:
        print('Error: Problem fetching station info from Flask')
        hourly_risk = {}

    #get weather forecast
    try:
        weather_res = requests.get(f"{FLASK_API_URL}/location_forecast", params={"station_code": station_code})
        weather_res.raise_for_status()
        forecast_data = weather_res.json()
    except Exception:
        print('Error: Problem fetching weather forecast from Flask')
        hourly_risk = []

    route = get_route_code(station_code).lower()

    #get risk forecast
    try:
        risk_res = requests.get(f"{FLASK_API_URL}/delay_risk", params={"station_code": station_code})
        risk_res.raise_for_status()
        risk_data = risk_res.json()
        hourly_risk = risk_data.get("hourly_risk", [])
    except Exception:
        print('Error: Problem fetching risk forecast from Flask')
        hourly_risk = []

    #get the next 5 days as dates (including today)
    tz = ZoneInfo("Europe/London")
    today = datetime.now(tz).date()
    timeframe = [today + timedelta(days=i) for i in range(5)]
    forecast_days = {day.isoformat(): [] for day in timeframe}

    #get the risk for hours within the window 6am - 11pm
    for hour in hourly_risk:                     
        timestamp = hour["timestamp_utc"]
        timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if 6 <= timestamp.hour <= 23:
            date_key = timestamp.date().isoformat()
            if date_key in forecast_days:   
                forecast_days[date_key].append({
                    "timestamp_utc": timestamp, 
                    "weather": hour["features"], 
                    "probs": hour["probs"],
                    "top_features": hour.get("top_features")
                    })
        

    for date_key in forecast_days:
        forecast_days[date_key].sort(key=lambda x: x['timestamp_utc'])

    days = sorted(forecast_days.keys())[:5]

    #get current conditions
    current_conditions = forecast_data["hourly_forecasts"][0]
    weather_code = int(current_conditions.get("weather_code", 0))
    is_day = bool(current_conditions.get("is_day", 1))
    gusts = float(current_conditions.get("gusts", 0.0))
    icon = determine_icon(weather_code, is_day, gusts)
    description = determine_description(weather_code, gusts)

    return {
        "station_info": station_info,
        "forecast_data": forecast_data,
        "hourly_risk": hourly_risk,
        "forecast_by_day": forecast_days,
        "days": days,
        "current_conditions": current_conditions,
        "icon": icon,
        "description": description,
    }

def determine_weather_label(raw_name):
    
    ui_labels = {
        "temp_2m": "Temperature",
        "relative_humidity": "Humidity",
        "rain": "Rain",
        "gusts": "Wind Gusts",
        "snow_depth": "Snow Depth",
        "surface_pressure": "Pressure",
    }
    return ui_labels.get(raw_name)



