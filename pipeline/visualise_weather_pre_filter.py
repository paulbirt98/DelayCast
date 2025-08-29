import pandas as pd
from pipeline_utils.config import UNIFIED_ROUTES_FILE, INDIVIDUAL_ROUTES, UNIFIED_ROUTES_DIR, FS_RESULTS
import numpy as np
from pipeline_utils.visualisation_helpers import plot_stacked_delay_by_binned_feature
import argparse

def argparse_cl_arguments():
    """
        parses command-line arguments so they are accessible within the add_new_route script

        Args:
        - read from command line

        Command-line Arguments:
        - --route(str): the route identifier in format 'glq_inv.

        Returns:
        - parser.parse_args(): the parsed arguments accessible as attributes.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", type=str, default=None)
    return parser.parse_args()

if __name__ == "__main__":

    args = argparse_cl_arguments()
    route = args.route

    if route:
        route = route.lower()
        dataset = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_training_data.csv')
        folder = FS_RESULTS / route
        folder.mkdir(parents=True, exist_ok=True)
        temp_chart_filepath = folder / f'{route}_temp_stacked_barchart.png'
        snow_chart_filepath = folder / f'{route}_snow_depth_stacked_barchart.png'
        rain_chart_filepath = folder / f'{route}_rain_stacked_barchart.png'
        gusts_chart_filepath = folder / f'{route}_gusts_stacked_barchart.png'
        stats_filepath = folder /f'{route}_weather_stats.txt'
    else:
        route = 'all'
        dataset = pd.read_csv(UNIFIED_ROUTES_DIR / 'unified_training_data.csv')
        folder = FS_RESULTS / route
        folder.mkdir(parents=True, exist_ok=True)
        temp_chart_filepath = folder / f'{route}_temp_stacked_barchart.png'
        snow_chart_filepath = folder / f'{route}_snow_depth_stacked_barchart.png'
        rain_chart_filepath = folder / f'{route}_rain_stacked_barchart.png'
        gusts_chart_filepath = folder / f'{route}_gusts_stacked_barchart.png'
        stats_filepath = folder /f'{route}_weather_stats.txt'

#start writing stats to txt file
with open(stats_filepath, 'w') as file:

    #get dataset full rows for comaparison with value counts
    file.write(f'Total number of records: {len(dataset)} \n\n')
    delay_counts = dataset['delay_classification'].value_counts().reset_index()
    delay_counts.columns = ['delay_classification', 'count']
    file.write(delay_counts.to_string(index=False))

    file.write('\n\n')

    domain_important_weather_features = ['temperature_2m', 'rain', 'wind_gusts_10m', 'snow_depth']

    for feature in domain_important_weather_features:

        file.write(f'{feature} stats \n')
        file.write(dataset[f'{feature}'].describe().to_string(index=False))
        file.write('\n\n')


    # Temperature bins
    temp_bins = [-float('inf'), -5, 0, 10, 20, 25, 30, float('inf')]
    temp_labels = ['Very Cold', 'Cold', 'Cool', 'Mild', 'Warm', 'Hot', 'Very Hot']

    dataset['temp_bin'] = pd.cut(dataset['temperature_2m'], bins=temp_bins, labels=temp_labels)

    temp_counts = dataset['temp_bin'].value_counts().sort_index()
    file.write("Temperature bin counts:\n")
    file.write(temp_counts.to_string())

    file.write('\n\n')

    # Snow depth bins
    snow_bins = [-0.01, 0, 0.03, 0.1, float('inf')]
    snow_labels = ['No Snow', 'Light', 'Moderate', 'Heavy']

    dataset['snow_bin'] = pd.cut(dataset['snow_depth'], bins=snow_bins, labels=snow_labels)

    snow_counts = dataset['snow_bin'].value_counts().sort_index()
    file.write("Snow bin counts:\n")
    file.write(snow_counts.to_string())

    file.write('\n\n')

    # Rain bins
    rain_bins = [-0.01, 0, 0.25, 3, float('inf')]
    rain_labels = ['None', 'Light', 'Moderate', 'Heavy']

    dataset['rain_bin'] = pd.cut(dataset['rain'], bins=rain_bins, labels=rain_labels)

    rain_counts = dataset['rain_bin'].value_counts().sort_index()
    file.write("Rain bin counts:\n")
    file.write(rain_counts.to_string())

    file.write('\n\n')

    # Wind gust bins
    wind_bins = [0, 20, 40, 60, 80, float('inf')]
    wind_labels = ['Calm', 'Breezy', 'Windy', 'Gusty', 'Severe']

    dataset['wind_bin'] = pd.cut(dataset['wind_gusts_10m'], bins=wind_bins, labels=wind_labels)

    wind_counts = dataset['wind_bin'].value_counts().sort_index()
    file.write("Wind gust bin counts:\n")
    file.write(wind_counts.to_string())

plot_stacked_delay_by_binned_feature(dataset, 'temperature_2m', temp_bins, temp_labels, filepath=temp_chart_filepath)
plot_stacked_delay_by_binned_feature(dataset, 'snow_depth', snow_bins, snow_labels, filepath=snow_chart_filepath)
plot_stacked_delay_by_binned_feature(dataset, 'wind_gusts_10m', wind_bins, wind_labels, filepath=gusts_chart_filepath)
plot_stacked_delay_by_binned_feature(dataset, 'rain', rain_bins, rain_labels, filepath=rain_chart_filepath)


