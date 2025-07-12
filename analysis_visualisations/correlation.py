import seaborn as sb
import matplotlib.pyplot as mpl
import pandas as pd

df = pd.read_csv('../data/processed/eus_liv_joined.csv')

correlation_matrix = df[['temperature_2m', 'snowfall', 'snow_depth', 'rain','apparent_temperature', 'cloud_cover', 'wind_speed_10m', 'wind_gusts_10m']].corr()

mpl.figure(figsize=(10,8))
sb.heatmap(correlation_matrix, annot=True, fmt='.2f', cmap='coolwarm')
mpl.title('Correlation Matrix for Numeric Features')
mpl.xticks(rotation=45)
mpl.show()