import pandas as pd
from utils import weather_call

df = pd.read_csv('eus_liv_w_derived_features.csv', parse_dates=['Actual Time'])

station_coordinates = {
    'EUS': {'latitude': 51.5281, 'longitude': -0.1331},
    'WFJ': {'latitude': 51.6635, 'longitude': -0.3961},
    'MKC': {'latitude': 52.0345, 'longitude': -0.7738},
    'RUG': {'latitude': 52.3786, 'longitude': -1.2508},
    'NUN': {'latitude': 52.5263, 'longitude': -1.4641},
    'TAM': {'latitude': 52.6374, 'longitude': -1.6871},
    'LTV': {'latitude': 52.6869, 'longitude': -1.8003},
    'STA': {'latitude': 52.8039, 'longitude': -2.1221},
    'CRE': {'latitude': 53.0895, 'longitude': -2.4336},
    'RUN': {'latitude': 53.3387, 'longitude': -2.7393},
    'LIV': {'latitude': 53.4076, 'longitude': -2.9773}
}

#placeholder list for weather dataframes
weather_responses = []

for station in station_coordinates.keys():
	latitude = station_coordinates[station]['latitude']
	longitude = station_coordinates[station]['longitude']

	#get start and end dates for station
	station_df = df[df['Station'] == station]
	start_date = station_df['Actual Time'].min().strftime('%Y-%m-%d')
	end_date = station_df['Actual Time'].max().strftime('%Y-%m-%d')

	
	station_weather = weather_call(start_date, end_date, latitude, longitude, station)
	

	weather_responses.append(station_weather)

joined_weather_data = pd.concat(weather_responses, ignore_index=True)

joined_weather_data.to_csv("eus_liv_weather.csv", index=False)