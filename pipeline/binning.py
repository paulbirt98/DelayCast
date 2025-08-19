import pandas as pd
from pipeline_utils.config import UNIFIED_ROUTES_FILE, UNIFIED_ROUTES_DIR, INDIVIDUAL_ROUTES
from pipeline_utils.preproccesing_helpers import calculate_delay_classification
from pipeline_utils.eng_helpers import tvt_split
from pipeline_utils.visualisation_helpers import plot_stacked_delay_by_binned_feature

#assign route
route = 'glq_inv'
route_filepath = INDIVIDUAL_ROUTES / route / f'{route}_route.csv'

#read file and recalculate delay classes
data = pd.read_csv(route_filepath)
data['delay_classification'] = data['delay_minutes'].apply(calculate_delay_classification)

# Temperature bins
temp_bins = [-float('inf'), -3, 2, 10, 20, 23, float('inf')]
temp_labels = ['Very Cold', 'Cold', 'Cool', 'Mild', 'Warm', 'Hot']

data['temp_bin'] = pd.cut(data['temperature_2m'], bins=temp_bins, labels=temp_labels)

temp_counts = data['temp_bin'].value_counts().sort_index()
print("Temperature bin counts:\n")
print(temp_counts.to_string())

plot_stacked_delay_by_binned_feature(data, 'temperature_2m', temp_bins, temp_labels)

print('\n\n')

# Snow depth bins
snow_bins = [-0.01, 0, 0.03, 0.1, float('inf')]
snow_labels = ['No Snow', 'Dusting', 'Substantial', 'Deep']

data['snow_depth_bin'] = pd.cut(data['snow_depth'], bins=snow_bins, labels=snow_labels)

snow_counts = data['snow_depth_bin'].value_counts().sort_index()
print("Snow bin counts:\n")
print(snow_counts.to_string())

plot_stacked_delay_by_binned_feature(data, 'snow_depth', snow_bins, snow_labels)

print('\n\n')

# Rain bins
rain_bins = [-0.01, 0, 0.5, 1, float('inf')]
rain_labels = ['No Rain', 'Light', 'Heavy', 'Very Heavy']

data['rain_bin'] = pd.cut(data['rain'], bins=rain_bins, labels=rain_labels)

rain_counts = data['rain_bin'].value_counts().sort_index()
print("Rain bin counts:\n")
print(rain_counts.to_string())

plot_stacked_delay_by_binned_feature(data, 'rain', rain_bins, rain_labels)

print('\n\n')

# Wind gusts
gust_bins = [0, 20, 40, 60, 75, float('inf')]
gust_labels = ['Calm', 'Breezy', 'Windy', 'Gale Force', 'Severe Gale']

data['gust_bin'] = pd.cut(data['wind_gusts_10m'], bins=gust_bins, labels=gust_labels)

gust_counts = data['gust_bin'].value_counts().sort_index()
print("Wind gust bin counts:\n")
print(gust_counts.to_string())

plot_stacked_delay_by_binned_feature(data, 'wind_gusts_10m', gust_bins, gust_labels)

# Surface pressure
pressure_bins = [-float('inf'), 980, 1000, 1020, float('inf')]
pressure_labels = ['Very Low', 'Low', 'Normal', 'High']

data['pressure_bin'] = pd.cut(data['surface_pressure'], bins=pressure_bins, labels=pressure_labels)

pressure_counts = data['pressure_bin'].value_counts().sort_index()
print("Surface Pressure bin counts:\n")
print(pressure_counts.to_string())

plot_stacked_delay_by_binned_feature(data, 'surface_pressure', pressure_bins, pressure_labels)

#save over file
data.to_csv(INDIVIDUAL_ROUTES / route / 'binned' / f'{route}_binned.csv', index=False)


