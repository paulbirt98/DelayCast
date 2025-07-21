import argparse
import pandas as pd
from pipeline_utils.config import RAW_DATA, INTERIM_DATA, METADATA, UK_STATIONS_FILE
from pipeline_utils.preproccesing_helpers import ( 
    convert_to_datetime, 
    to_long_format,
    save_recent_and_frequent, 
    create_station_coords_json, 
    clean, 
    derive_temporal_features,
    get_direction_feature,
    calculate_delay,
    calculate_delay_classification,
    get_weather_data,
    join_train_weather_data
)

def parse_cl_arguments():
    """
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--from_location", type=str, required=True)
    parser.add_argument("--to_location", type=str, required=True)
    return parser.parse_args()

#assign command line args to variables
args = parse_cl_arguments()
from_location = args.from_location.lower()
to_location = args.to_location.lower()

#get the relevant raw journey data filepath
raw_data_file = RAW_DATA / f"{from_location}_{to_location}_raw.csv"

#load this to pandas
try:
    journeys_df = pd.read_csv(raw_data_file)
except FileNotFoundError:
    print(f"Error: File {raw_data_file} not found")
    raise
except pd.errors.ParserError:
    print(f"Error parsing file {raw_data_file}")
    raise
except PermissionError:
    print(f"Permission Error with file {raw_data_file}. Ensure the file is not open elsewhere.")
    raise
except Exception as e:
    print(f"Unexpected error reading file {raw_data_file}: {e}")

#convert time columns to datetime
journeys_df = convert_to_datetime(journeys_df)

#convert to long format - one row/record per stopping
print(f"Shape before converting to long format: {journeys_df.shape}")
stoppings_df = to_long_format(journeys_df, from_location, to_location)
print(f"Shape after converting to long format: {stoppings_df.shape}")

#drop low frequency stations and stations with no recent recorsd
print(f"Shape before dropping low frequency stations: {stoppings_df.shape}")
stoppings_df, station_codes = save_recent_and_frequent(stoppings_df, from_location, to_location)
print(f"Shape after dropping low frequency stations: {stoppings_df.shape}")
print("Station codes being used:", station_codes)

#clean
print(f"Shape before cleaning dataframe: {stoppings_df.shape}")
stoppings_df = clean(stoppings_df)
print(f"Shape after cleaning dataframe: {stoppings_df.shape}")

print(f"Shape before deriving + calculating features: {stoppings_df.shape}")

#derive temporal and direction features
stoppings_df = derive_temporal_features(stoppings_df)
stoppings_df = get_direction_feature(stoppings_df, from_location, to_location)

#calculate delay minutes
stoppings_df['delay_minutes'] = stoppings_df.apply(
    lambda row: calculate_delay(row['scheduled_time'], row['actual_time']), axis=1
)

#classify based on minutes
stoppings_df['delay_classification'] = stoppings_df.apply(
    lambda row: calculate_delay_classification(row['delay_minutes']), axis=1
)

print(f"Shape after deriving + calculating features: {stoppings_df.shape}")

#get filepath and save a copy with all derived features
with_features_filepath = INTERIM_DATA / f'{from_location}_{to_location}_w_derived_features.csv'
stoppings_df.to_csv(with_features_filepath, index=False)

#pull the saved station coords from the metadata and create a dictionary
uk_station_coords_df = pd.read_csv(UK_STATIONS_FILE)
station_coords = create_station_coords_json(station_codes, uk_station_coords_df, from_location, to_location)

#get weather data and join
print(f"Shape before merging with weather: {stoppings_df.shape}")

weather_df = get_weather_data(stoppings_df, station_coords, from_location, to_location)

merged_df = join_train_weather_data(stoppings_df, weather_df, from_location, to_location)

print(f"Shape after merging with weather: {merged_df.shape}")

