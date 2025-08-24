import requests
from web_app.config import FLASK_API_URL
from web_app.frontend.icon_logic import WEATHER_ICON_MAP, WEATHER_ICON_DIR
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

def add_latest_weather(station_data):
    """
    
    """

    for station in station_data:
        try:
            res = requests.get(f'{FLASK_API_URL}/get_location_forecast', params={'station_code': station['station_code']})
            res.raise_for_status()
            
            forecast = res.json()

            #get most recent
            current_hour = forecast['hourly_forecasts'][0]
            station['timestamp'] = current_hour['timestamp']

            weather_code = int(current_hour['weather_code'])
            day = bool(current_hour['is_day'])

            station['weather_code'] = weather_code
            station['is_day'] = day

            if day:
                icon_filepath = WEATHER_ICON_DIR / WEATHER_ICON_MAP.get(weather_code)['day']
            else:
                icon_filepath = WEATHER_ICON_DIR / WEATHER_ICON_MAP.get(weather_code)['night']
            
            bytes = (icon_filepath).read_bytes()
            station['icon'] = "data:image/svg+xml;base64," + base64.b64encode(bytes).decode("ascii")

        except:
            pass

    return station_data