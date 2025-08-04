import pandas as pd
from pipeline_utils.config import UNIFIED_ROUTES_FILE, INDIVIDUAL_ROUTES, UNIFIED_ROUTES_DIR, FS_RESULTS
import numpy as np
from pipeline_utils.visualisation_helpers import plot_stacked_delay_by_binned_feature
import argparse

def argparse_cl_arguments():
    """
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", type=str, default=None)
    return parser.parse_args()

if __name__ == "__main__":

    args = argparse_cl_arguments()
    route = args.route.lower()

    if route:
        dataset = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_training_data.csv')
        folder = FS_RESULTS / route
        folder.mkdir(parents=True, exist_ok=True)
        temp_chart_filepath = folder / f'{route}_temp_stacked_barchart.png'
        snow_chart_filepath = folder / f'{route}_snow_depth_stacked_barchart.png'
        rain_chart_filepath = folder / f'{route}_rain_stacked_barchart.png'
        gusts_chart_filepath = folder / f'{route}_gusts_stacked_barchart.png'
        stats_filepath = folder /f'{route}_weather_stats.txt'
    else:
        dataset = pd.read_csv(UNIFIED_ROUTES_DIR)

#get dataset full rows for comaparison with value counts
print(f'Total number of records: {len(dataset)}')
print('\n')

domain_important_weather_features = ['temperature_2m', 'rain', 'wind_gusts_10m', 'snow_depth']

for feature in domain_important_weather_features:

    print(f'{feature} stats')
    print(dataset[f'{feature}'].describe())
    print('\n')


# Temperature bins
temp_bins = [-float('inf'), -5, 0, 10, 20, 25, 30, float('inf')]
temp_labels = ['Very Cold', 'Cold', 'Cool', 'Mild', 'Warm', 'Hot', 'Very Hot']

dataset['temp_bin'] = pd.cut(dataset['temperature_2m'], bins=temp_bins, labels=temp_labels)

temp_counts = dataset['temp_bin'].value_counts().sort_index()
print("Temperature bin counts:")
print(temp_counts)


# Snow depth bins
snow_bins = [-0.01, 0, 0.03, 0.1, float('inf')]
snow_labels = ['No Snow', 'Light', 'Moderate', 'Heavy']

dataset['snow_bin'] = pd.cut(dataset['snow_depth'], bins=snow_bins, labels=snow_labels)

snow_counts = dataset['snow_bin'].value_counts().sort_index()
print("Snow bin counts:")
print(snow_counts)


# Rain bins
rain_bins = [-0.01, 0, 0.25, 3, float('inf')]
rain_labels = ['None', 'Light', 'Moderate', 'Heavy']

dataset['rain_bin'] = pd.cut(dataset['rain'], bins=rain_bins, labels=rain_labels)

rain_counts = dataset['rain_bin'].value_counts().sort_index()
print("Rain bin counts:")
print(rain_counts)



# Wind gust bins
wind_bins = [0, 20, 40, 60, 80, float('inf')]
wind_labels = ['Calm', 'Breezy', 'Windy', 'Gusty', 'Severe']

dataset['wind_bin'] = pd.cut(dataset['wind_gusts_10m'], bins=wind_bins, labels=wind_labels)

wind_counts = dataset['wind_bin'].value_counts().sort_index()
print("Wind gust bin counts:")
print(wind_counts)

plot_stacked_delay_by_binned_feature(dataset, 'temperature_2m', temp_bins, temp_labels, filepath=temp_chart_filepath)
plot_stacked_delay_by_binned_feature(dataset, 'snow_depth', snow_bins, snow_labels, filepath=snow_chart_filepath)
plot_stacked_delay_by_binned_feature(dataset, 'wind_gusts_10m', wind_bins, wind_labels, filepath=gusts_chart_filepath)
plot_stacked_delay_by_binned_feature(dataset, 'rain', rain_bins, rain_labels, filepath=rain_chart_filepath)


