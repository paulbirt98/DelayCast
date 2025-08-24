from web_app.config import WEATHER_ICON_DIR, WEBAPP_DB, FLASK_API_URL
from web_app.database.db_utils.init_db import Station, HourlyForecast

#for use directly 
WIND = {'file': 'wind.svg', 'description': 'windy'}
THERMOMETER = 'thermometer.svg'
COLD = 'thermometer-colder.svg'
HOT = 'thermometer-warmer.svg'

#for use in map
PARTLY_CLOUDY = {'day': 'partly-cloudy-day.svg'}
FOG = {'day': 'fog.svg', 'night': 'fog.svg'}
DRIZZLE = {'day': 'drizzle.svg', 'night': 'drizzle.svg'}
SLEET = {'day': 'sleet.svg', 'night': 'sleet.svg'}
RAIN = {'day': 'rain.svg', 'night': 'rain.svg'}
SNOW = {'day': 'snow.svg', 'night': 'snow.svg'}
SHOWERS = {'day': 'partly-cloudy-day-rain.svg', 'night': 'partly-cloudy-night-rain.svg'}
SNOW_SHOWERS = {'day': 'partly-cloudy-day-snow.svg', 'night': 'partly-cloudy-night-snow.svg'}
THUNDERSTORM = {'day': 'thunderstorm-rain.svg', 'night': 'thunderstorms-rain.svg'}

#mapping code to icons
WEATHER_ICON_MAP = {
    0: {'day': 'clear-day.svg', 'night': 'clear-night.svg'},
    1: PARTLY_CLOUDY,
    2: PARTLY_CLOUDY,
    3: {'day': 'overcast.svg', 'night': 'overcast.svg'},
    45: FOG,
    48: FOG,
    51: DRIZZLE,
    53: DRIZZLE,
    55: DRIZZLE,
    56: SLEET,
    57: SLEET,
    61: DRIZZLE,
    63: RAIN,
    65: RAIN,
    66: SLEET,
    67: SLEET,
    71: SNOW,
    73: SNOW,
    75: SNOW,
    77: SNOW,
    80:  {'day': 'partly-cloudy-day-drizzle.svg', 'night': 'partly-cloudy-night-drizzle.svg'},
    81: SHOWERS,
    82: SHOWERS,
    85: SNOW_SHOWERS,
    86: SNOW_SHOWERS,
    95: THUNDERSTORM,
    96: THUNDERSTORM,
    99: THUNDERSTORM
}

WEATHER_DESCRIPTION_MAP = {
    0: 'Clear',
    1: 'Mostly Sunny',
    2: 'Partly Cloudy',
    3: 'Overcast',
    45: 'Fog',
    48: 'Fog',
    51: 'Light Drizzle',
    53: 'Drizzle',
    55: 'Dense Drizzle',
    56: 'Light Freezing Drizzle',
    57: 'Freezing Drizzle',
    61: 'Light Rain',
    63: 'Rain',
    65: 'Heavy Rain',
    66: 'Light Freezing Rain',
    67: 'Freezing Rain',
    71: 'Light Snow',
    73: 'Snow',
    75: 'Heavy Snow',
    77: 'Snow',
    80: 'Light Showere',
    81: 'Showers',
    82: 'Heavy Showers',
    85: 'Light Snow Showers',
    86: 'Heavy Snow Showers',
    95: 'Thunderstorm',
    96: 'Thunderstorm',
    99: 'Thunderstorm',
}
