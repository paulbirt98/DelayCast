import pandas as pd

df = pd.read_csv('../data/processed/eus_liv_joined.csv')

var_scores = df[['temperature_2m', 'snowfall', 'snow_depth', 'rain','apparent_temperature', 'cloud_cover', 'wind_speed_10m', 'wind_gusts_10m']].var().sort_values()

print(var_scores)