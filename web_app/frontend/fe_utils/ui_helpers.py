import requests
from web_app.config import FLASK_API_URL,WINDY
from web_app.frontend.icon_logic import WEATHER_ICON_MAP, WEATHER_ICON_DIR, WIND, WEATHER_DESCRIPTION_MAP
import base64

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

