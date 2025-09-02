import pandas as pd
from pipeline_utils.config import (
    INDIVIDUAL_ROUTES,
    TRAINING_RATIO,
    VALIDATION_RATIO,
    NO_DELAY_UPPER_BOUNDARY,
    MINOR_DELAY_UPPER_BOUNDARY,
    MODERATE_DELAY_UPPER_BOUNDARY,
)

def tvt_split(route):
    """
    Spltis the given route's full dataset into training, validation and testing subsets based on TRAINING_RATIO and
    VALIDATION_RATIO as set in pipeline_utils.config.py. The resulting sets are saved within the routes directory within the 
    INDIVIDUAL ROUTES directory.

    Args:
    - route (str): the route to be split, in the format 'glq_inv'
    """
    #assign and create file and directory paths accordingly
    route_directory = INDIVIDUAL_ROUTES / route
    route_directory.mkdir(parents=True, exist_ok=True)

    file = route_directory / f'{route}_route.csv'

    try:
        dataset = pd.read_csv(file, parse_dates=['scheduled_time'])
    except FileNotFoundError:
        print(f"Error: File {file} not found")
        raise
    except pd.errors.ParserError:
        print(f"Error parsing file {file}")
        raise
    except PermissionError:
        print(f"Permission Error with file {file}. Ensure the file is not open elsewhere.")
        raise
    except Exception as e:
        print(f"Unexpected error reading file {file}: {e}")

    training_file = route_directory / f'{route}_training_data.csv'
    validation_file = route_directory / f'{route}_validation_data.csv'
    testing_file = route_directory / f'{route}_testing_data.csv'

    #sort the rows by datetime on scheduled
    dataset = dataset.sort_values(by=['scheduled_time']).reset_index(drop=True) 

    #get total number of rows
    total = len(dataset)

    training_cutoff = int(total * TRAINING_RATIO)
    validation_cutoff = training_cutoff + int(total * VALIDATION_RATIO)

    #get dataframes based on these cut offs
    training_df = dataset.iloc[:training_cutoff]
    validation_df = dataset.iloc[training_cutoff:validation_cutoff]
    testing_df = dataset.iloc[validation_cutoff:]

    print(f"Training: {training_df['scheduled_time'].min()} to {training_df['scheduled_time'].max()}")
    print(f"Validation: {validation_df['scheduled_time'].min()} to {validation_df['scheduled_time'].max()}")
    print(f"Testing: {testing_df['scheduled_time'].min()} to {testing_df['scheduled_time'].max()}")


    #save to appropriate directories
    training_df.to_csv(training_file, index=False)
    validation_df.to_csv(validation_file, index=False)
    testing_df.to_csv(testing_file, index=False)

def sample_for_training(route, training_df, low_temperature, high_temperature, high_gusts, low_pressure, high_pressure, heavy_rain):
    """
    
    """
    print(f'\nNumber of rows in {route} training data: {len(training_df)}')

    #identofy minority classes to preserve
    mild_rows = training_df[training_df['delay_classification'] == 'Mild Delay']
    moderate_rows = training_df[training_df['delay_classification'] == 'Moderate Delay']
    severe_rows = training_df[training_df['delay_classification'] == 'Severe Delay']

    print(f"Keeping {len(mild_rows)} Mild, {len(moderate_rows)} Moderate, {len(severe_rows)} Severe")

    minority_rows = pd.concat([mild_rows, moderate_rows, severe_rows], ignore_index=False)

    #remove minority classes from main df
    minority_rows_records = minority_rows.index
    unprotected_records = training_df.drop(minority_rows_records)

    #identify rare but important weather variables
    rare_weather = unprotected_records[
        (unprotected_records['snow_depth'] > 0) |
        (unprotected_records['temperature_2m'] < low_temperature) |
        (unprotected_records['temperature_2m'] > high_temperature) |
        (unprotected_records['wind_gusts_10m'] > high_gusts) |
        (unprotected_records['surface_pressure'] < low_pressure) |
        (unprotected_records['surface_pressure'] > high_pressure) |
        (unprotected_records['rain'] > heavy_rain)
    ]

    #remove rare weather to sample equal number from each route
    rare_weather_records = rare_weather.index
    unprotected_records = unprotected_records.drop(rare_weather_records)

    #sample from no delays and no bad weather to max 40% total number of records
    sample_data = unprotected_records.sample(n=min(int(len(training_df) * 0.4), len(unprotected_records)), random_state=42)
    print(f'Number of rows sampled {len(sample_data)}')

    #combine with rare weather
    final_sample = pd.concat([sample_data, rare_weather, minority_rows], ignore_index=True)

    print(f'{len(rare_weather)} rare event rows added back for {route}')
    print(f"{len(mild_rows)} Mild, {len(moderate_rows)} Moderate and {len(severe_rows)} Severe delays added back for {route}")
    print(f'Final sample size for {route}: {len(final_sample)} rows')

    # Drop any rows with at least one NaN
    final_sample_clean = final_sample.dropna()

    print(f"Dropped {len(final_sample) - len(final_sample_clean)} rows with NaNs.")

    final_sample_clean.to_csv(INDIVIDUAL_ROUTES / route / 'binned' / f'{route}_binned_training_data.csv', index=False)

def threeclass_delay_classification(delay_minutes):
    """
    Calculates delays as either 'No Delay', 'Minor Delay' or 'Major Delay' given the delay_minutes.

    Args:
    - delay_minutes (int): the number of minutes a train was delayed by

    Returns:
    - If delay_minutes is NA, 'Issue Classifying'. If delay_minutes less than NO_DELAY_UPPER_BOUNDARY, 'No Delay'. If
    delay_minutes less than MINOR_DELAY_UPPER_BOUNDARY, 'Minor Delay. If delay_minutes greater than 15, 'Major Delay.
    
    """
    if pd.isna(delay_minutes):
        return "Issue Classifying"
    elif delay_minutes < NO_DELAY_UPPER_BOUNDARY:
        return "No Delay"
    elif NO_DELAY_UPPER_BOUNDARY <= delay_minutes < MINOR_DELAY_UPPER_BOUNDARY:
        return "Minor Delay"
    elif 15 <= delay_minutes:
        return "Major Delay"
    
def binary_delay_classification(delay_minutes):
    """
    Calculates delays as either 'No Delay' or 'Delay' given the delay_minutes.

    Args:
    - delay_minutes (int): the number of minutes a train was delayed by.

    Returns:
    - If delay_minutes is NA, 'Issue Classifying'. If delay_minutes less than NO_DELAY_UPPER_BOUNDARY, 'No Delay'. If
    delay_minutes greater than NO_DELAY_UPPER_BOUNDARY, 'Delay'.
    """
    if pd.isna(delay_minutes):
        return "Issue Classifying"
    elif delay_minutes < NO_DELAY_UPPER_BOUNDARY:
        return "No Delay"
    elif NO_DELAY_UPPER_BOUNDARY <= delay_minutes:
        return "Delay"
    