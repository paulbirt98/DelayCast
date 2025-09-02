import pandas as pd
from tqdm import tqdm
from pipeline_utils.config import (
    FREQ_VALUE, 
    CUT_OFF_DATE,
    RECORDING_ERROR_MIN, 
    RECORDING_ERROR_MAX, 
    NO_DELAY_UPPER_BOUNDARY, 
    MINOR_DELAY_UPPER_BOUNDARY, 
    MODERATE_DELAY_UPPER_BOUNDARY, 
    METADATA, 
    RAW_DATA,
    INTERIM_DATA,
    INDIVIDUAL_DIRECTIONS,
    NUMERICAL_FEATURES
)
import json
import openmeteo_requests
import requests_cache
from retry_requests import retry
import time

tqdm.pandas()

def convert_to_datetime(journeys_df):
    """
    Converts all time columns (columns for shceduled and actual arrival or departure time) to datetime
    in the format YYYY-MM-DD HH:MM. If a field is missing it is set to NaT.

    Args:
    - journeys_df (dataframe): a dataframe containing details of all train journeys on a given route

    Returns:
    - journeys_df (dataframe): the same dataframe passed in as an argument but with all time columns converted to datetime
    """
    print("Converting time columns to datetime")

    time_columns = [col for col in journeys_df.columns if '_time' in col]

    for col in time_columns:
        journeys_df[col] = journeys_df.progress_apply(
            lambda row: pd.to_datetime(f"{row['date']} {int(row[col]):04}", format="%Y-%m-%d %H%M") if pd.notnull(row[col]) else pd.NaT,
            axis=1
        )

    print("Converted to datetime")

    return journeys_df

def load_json_to_dict(from_location, to_location):
    """
    Loads the relevant json file to the script containing stations on a route, given the from_location and to_location passed in.

    Args:
    - from_location (str): the origin station code
    - to_location (str): the terminus station code

    Returns:
    - station_codes (dict): A dictionary of all saved station codes on a route and their respective coordinates
    """
    file_path = METADATA / f"{from_location}_{to_location}_stations.json"

    try:
        with open(file_path, "r") as file:
            station_codes = json.load(file)
        
        return station_codes
    
    except FileNotFoundError:
        print(f"Error loading json file. File not found at path: {file_path}")
        return {}
    except PermissionError:
        print(f"Permission error in accessing file at {file_path}, please ensure it is not open elsewhere")
        return {}
    except Exception as e:
        print(f"Unexcpeted error loading file at {file_path}: {e}")
        return {}

def to_long_format(journeys_df, from_location, to_location):
    """
    Converts a dataframe of train journeys to long format. I.e. each row now relates to a single train passing a single station, 
    rather than a row representing an entire journey.

    Rows are only included where at least one of the scheduled or actual time values is recorded for a given station.

    Args:
    - journeys_df (dataframe): a dataframe containing details of all train journeys on a given route
    - from_location (str): the origin station code
    - to_location (str): the terminus station code

    Returns:
    - stoppings_df (dataframe): a long format dataframe with one row per stopping record, containing the columns:
        ['rid', 'date', 'toc', 'station', 'scheduled_time', 'actual_time', 'lc_reason']
    """
    id_vars = ['rid', 'date', 'toc']
    long_format_rows = []

    # Get all unique station codes based on scheduled/actual time columns
    station_codes = sorted({
        col.split('_')[0]
        for col in journeys_df.columns
        if '_scheduled_' in col or '_actual_' in col
    })

    print("Converting to long format")

    for station in station_codes:
        
        # Determine correct arrival/departure field names
        if station == to_location:
            scheduled_col = f"{station}_scheduled_arrival_time"
            actual_col = f"{station}_actual_arrival_time"
        else:
            scheduled_col = f"{station}_scheduled_departure_time"
            actual_col = f"{station}_actual_departure_time"

        reason_col = f"{station}_lc_reason"
        
        # Check which columns exist in the input
        columns_present = [col for col in [scheduled_col, actual_col, reason_col] if col in journeys_df.columns]
        if not columns_present:
            print(f"Skipping {station}: no columns found")
            continue

        # Subset and rename
        station_df = journeys_df[id_vars + columns_present].copy()
        station_df['station'] = station.upper()

        rename_map = {}
        if scheduled_col in station_df.columns:
            rename_map[scheduled_col] = 'scheduled_time'
        if actual_col in station_df.columns:
            rename_map[actual_col] = 'actual_time'
        if reason_col in station_df.columns:
            rename_map[reason_col] = 'lc_reason'
        station_df.rename(columns=rename_map, inplace=True)

        # Drop rows where all available time columns are missing
        time_cols = [col for col in ['scheduled_time', 'actual_time'] if col in station_df.columns]
        if time_cols:
            station_df.dropna(subset=time_cols, how='all', inplace=True)

        long_format_rows.append(station_df)

    # Concatenate all station-level records
    stoppings_df = pd.concat(long_format_rows, ignore_index=True)

    stoppings_df = stoppings_df[~stoppings_df['rid'].isin(stoppings_df[stoppings_df['station'] == 'ABD']['rid'])]


    # Ensure consistent column order
    expected_cols = ['rid', 'date', 'toc', 'station', 'scheduled_time', 'actual_time', 'lc_reason']
    for col in expected_cols:
        if col not in stoppings_df.columns:
            stoppings_df[col] = pd.NA
    stoppings_df = stoppings_df[expected_cols]

    print("Checking for duplicates...")
    dupes = stoppings_df.duplicated(subset=['rid', 'station', 'scheduled_time', 'actual_time'])
    print(f"Duplicate rows: {dupes.sum()} out of {len(stoppings_df)}")

    print('dropping duplicates')
    stoppings_df = stoppings_df.drop_duplicates(subset=['rid', 'station', 'scheduled_time', 'actual_time'], keep='first')

    # Save and return
    filepath = INTERIM_DATA / f'{from_location}_{to_location}_long_format.csv'
    stoppings_df.to_csv(filepath, index=False)

    print(f"Saved long format to {filepath}")

    return stoppings_df

def get_first_and_terminus(stoppings_df):
    """
    Adds two new columns to the passed dataframe, 'is_first_station' and 'is_terminus'. If a stopping is the first of a given service (RID)
    is_first_station is set to 1, otherwise 0. If a stopping is the last of a given service, 'is_terminus' is set to 1, otherwise 0.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station

    Returns:
    - stoppings_df (dataframe): the same dataframe passed in as an argument but with the newly created and filled 'is_first_station' and 'is_terminus'
    columns.
    """
    # Sort by rid and scheduled time (or actual time if preferred)
    stoppings_df = stoppings_df.sort_values(by=["rid", "scheduled_time"])

    # For each rid, assign 1 to the first and last row for is_first_station and is_terminus
    stoppings_df['is_first_station'] = 0
    stoppings_df['is_terminus'] = 0

    first_indices = stoppings_df.groupby('rid').head(1).index
    last_indices = stoppings_df.groupby('rid').tail(1).index

    stoppings_df.loc[first_indices, 'is_first_station'] = 1
    stoppings_df.loc[last_indices, 'is_terminus'] = 1

    return stoppings_df

def save_recent_and_frequent(stoppings_df, from_location, to_location):
    """
    Removes any rows relating to stations which appear less frequently (as a percentage of total number of unique journey RIDs)
    than FREQ_THRESHOLD as set in config.py, as well as any rows for stations with no stoppings after CUT_OFF_DATE as set in config.py.
    The resulting dataframe is saved as a csv file in data/semi_processed.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station
    - from_location (str): the origin station code
    - to_location (str): the terminus station code

    Returns:
    - stoppings_df (dataframe): the same dataframe passed in as an argument but with all low frequency and non-recent stations removed
    - saved_stations (list[str]): a list of all unique station codes that exceed the threshold for inclusion
    """
    #calculate minimum frequency to be saved
    unique_journey_count = stoppings_df['rid'].nunique()

    frequency_threshold = unique_journey_count * FREQ_VALUE
    print(f"Minimum frequency to be saved {frequency_threshold}")

    #get the frequency for each station
    station_frequencies = stoppings_df['station'].value_counts()
    print("Station Frequencies")
    print(station_frequencies)

    #keep stations above thresholds
    frequent_stations = station_frequencies[station_frequencies >= frequency_threshold].index
    recent_stations = stoppings_df[stoppings_df['actual_time'] >= CUT_OFF_DATE]['station'].unique()
    saved_stations = sorted(set(frequent_stations) & set(recent_stations))
    stoppings_df = stoppings_df[stoppings_df['station'].isin(saved_stations)]
    
    print("Retained stations")
    print(saved_stations)

    return stoppings_df, saved_stations

def create_station_coords_json(station_codes, coords_df, from_location, to_location):
    """
    creates and saves a json file to the data/metadata folder containing each station in the final dataset for a given route
    along with its latitude and longitude. a dictionary representation of this is returned.

    Args:
    - station_codes (list[str]): a list of station codes
    - coords_df (dataframe): a dataframe containing all UK station codes and their corresponding coordinates
    - from_location (str): the origin station code
    - to_location (str): the terminus station code

    Returns:
    - station_coords_dict (dict): a dictionary of each station code along a route and its corresponding coordinates
    
    """
    #filepath to save json's to
    file_path = METADATA / f"{from_location}_{to_location}_stations.json"

    #filter for only the rows where the station is in the final dataset for the relevant route
    station_coords_df = coords_df[coords_df['crs_code'].isin(station_codes)]

    #create a dictionary format for saving as json
    station_coords_dict = {}

    for _, row in station_coords_df.iterrows():
        name = row['station_name']
        code =  row['crs_code']
        latitude = row['lat']
        longitude = row['long']
        station_coords_dict[name] = {'station_code': code, 'latitude': latitude, 'longitude': longitude}

    with open(file_path, "w") as json_file:
        json.dump(station_coords_dict, json_file)

    print('Coords dictionary created and saved to metadata as json')

    return station_coords_dict

def clean(stoppings_df):
    """
    Cleans a dataframe of train stoppings by enforcing the datatype of each feature as well as removing any rows with missing values 
    for TOC, Station, Scheduled Time or Actual Time. Also fills any rows with no late_cancellation reason code with 000 to represent none.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station

    Returns:
    - stoppings_df (dataframe): the same dataframe as was passed in but with missing values removed
    """
    stoppings_df['scheduled_time'] = pd.to_datetime(stoppings_df['scheduled_time'], errors='coerce')
    stoppings_df['actual_time'] = pd.to_datetime(stoppings_df['actual_time'], errors='coerce')

    stoppings_df['lc_reason'] = stoppings_df['lc_reason'].fillna('000').astype(str)
    stoppings_df['rid'] = stoppings_df['rid'].astype('Int64')

    stoppings_df['date'] = stoppings_df['date'].astype(str)
    stoppings_df['station'] = stoppings_df['station'].astype(str)
    stoppings_df['toc'] = stoppings_df['toc'].astype(str)

    stoppings_df.dropna(subset=['toc', 'station', 'scheduled_time', 'actual_time'], inplace=True)

    return stoppings_df

def derive_temporal_features(stoppings_df):
    """
    Derives temporal features (Hour, Day of the Week, and Month of the Year) and adds columns for each of these

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station

    Returns:
    - stoppings_df (dataframe): the same dataframe as was passed in but with columns representing temporal features added
    """
    stoppings_df['hour'] = stoppings_df['scheduled_time'].dt.hour
    stoppings_df['day'] = stoppings_df['scheduled_time'].dt.dayofweek
    stoppings_df['month'] = stoppings_df['scheduled_time'].dt.month

    return stoppings_df

def get_direction_features(stoppings_df, from_location, to_location):
    """
    Adds feature based directions, i.e. direction (example format for a EUS to LIV service: 'EUS-LIV').

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station
    - from_location (str): the origin station code
    - to_location (str): the terminus station code


    Returns:
    - stoppings_df (dataframe): the same dataframe as was passed in but with an added 'direction' feature  
    """
    stoppings_df['direction'] = f"{from_location.upper()}-{to_location.upper()}"

    return stoppings_df

def calculate_delay(scheduled_time, actual_time):
    """
    Calculates the delay at a station by comparing the scheduled time and actual time

    Args:
    - scheduled_time (datetime): this is the scheduled departure time, unless the station in question is the 
      terminus in which case this is the scheduled arrival time.
    - actual_time (datetime): this is the actual recorded departure time, unless the station in question is the 
      terminus in which case this is the actual recorded arrival time.

    Returns:
    the delay in minutes (float)
    """
    delay = (actual_time - scheduled_time).total_seconds() / 60

    #if more than 12 hours early  or late assume it was a recording error
    if delay < RECORDING_ERROR_MIN or delay > RECORDING_ERROR_MAX:  
        delay = None

    return delay

def calculate_delay_classification(delay_minutes):
    """
    Classifies delays into 4 categories (No Delay, Mild, Moderate and Severe) based on the delay_minutes as well as the 
    NO_DELAY_UPPER_BOUNDARY, MILD_DELAY_UPPER_BOUNDARY and MODERATE_DELAY_UPPER_BOUNDARY values as set in config.py.

    Args:
    - delay_minutes (float) : the delay in minutes

    Returns:
    - delay_classification (str): the delay classification
    """
    if pd.isna(delay_minutes):
        return "Issue Classifying"
    elif delay_minutes < NO_DELAY_UPPER_BOUNDARY:
        return "No Delay"
    elif NO_DELAY_UPPER_BOUNDARY <= delay_minutes < MINOR_DELAY_UPPER_BOUNDARY:
        return "Minor Delay"
    elif MINOR_DELAY_UPPER_BOUNDARY <= delay_minutes < MODERATE_DELAY_UPPER_BOUNDARY:
        return "Moderate Delay"
    elif delay_minutes >= MODERATE_DELAY_UPPER_BOUNDARY:
        return "Severe Delay"
    
def openmeteo_api_call(start_date, end_date, station_code, latitude, longitude):
    """
    Calls the openmeteo historical weather API to retrieve hourly weather variables for the given station location for the 
    given date range

    Args:
    - start_date (str): the first date in the date range to be requested (inclusive)
    - end_date (str): the last date in the date range to be requested (inclusive)
    - station_code (str): the CRS station code
    - latitude (float): the latitude of the location requested
    - longitude (float): the longitude of the location requested

    Returns:
    - hourly_dataframe (dataframe): a dataframe of weather for the given location and date range (hourly)
    """
    #variable to handle rate limit exceedance
    minute_rate_limit_message = "Minutely API request limit exceeded"
    hour_rate_limit_message = "Hourly API request limit exceeded"
    minute_wait_time = 90
    hour_wait_time = 3630
    retries = 100

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [     
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "rain",
            "snowfall",
            "snow_depth",
            "surface_pressure",
            "cloud_cover",
            "soil_temperature_0_to_7cm",
            "soil_moisture_0_to_7cm",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "is_day"
            ],
        "timezone": "Europe/London"
    }

    for tries in range(retries):
        try:
            responses = openmeteo.weather_api(url, params=params)
            break
        except Exception as e:
            print(f"Request failed: {e}")
            if minute_rate_limit_message in str(e):
                print(f"Minutely limit exceeded, waiting {minute_wait_time} seconds before retrying")
                time.sleep(minute_wait_time)
            elif hour_rate_limit_message in str(e):
                print(f"Hourly limit exceeded, waiting 1 hour before retrying")
                time.sleep(hour_wait_time)
            else:
                raise Exception("Issue calling weather API")


    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
    hourly_rain = hourly.Variables(4).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(5).ValuesAsNumpy()
    hourly_snow_depth = hourly.Variables(6).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(7).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(8).ValuesAsNumpy()
    hourly_soil_temperature_0_to_7cm = hourly.Variables(9).ValuesAsNumpy()
    hourly_soil_moisture_0_to_7cm = hourly.Variables(10).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(11).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(12).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(13).ValuesAsNumpy()
    hourly_is_day = hourly.Variables(14).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["dew_point_2m"] = hourly_dew_point_2m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["rain"] = hourly_rain
    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["snow_depth"] = hourly_snow_depth
    hourly_data["surface_pressure"] = hourly_surface_pressure
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["soil_temperature_0_to_7cm"] = hourly_soil_temperature_0_to_7cm
    hourly_data["soil_moisture_0_to_7cm"] = hourly_soil_moisture_0_to_7cm
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
    hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
    hourly_data["is_day"] = hourly_is_day

    hourly_dataframe = pd.DataFrame(data = hourly_data)

    #add column for station code
    hourly_dataframe['station'] = station_code

    return hourly_dataframe

def get_weather_data(stoppings_df, stations, from_location, to_location):
    """
    Creates a dataframe of hourly weather for each location along the route from the date of the first record in stoppings_df until the
    last. This is saved as a csv in the data/raw_api_responses directory.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station
    - stations (dictionary): a dictionary of each station code along a route and its corresponding coordinates
    - from_location (str): the origin station code
    - to_location (str): the terminus station code

    Returns:
    - joined_weather_df (dataframe): a dataframe of hourly weather for each location along the route.
    """
    print(f"Gathering weather data for {from_location} to {to_location}")

    #placeholder list for weather dataframes
    weather_dfs = []

    #loop through the stations
    for station in stations.keys():

        #get start and end dates for API call
        station_df = stoppings_df[stoppings_df['station'] == station]
        start_date = station_df['actual_time'].min().strftime('%Y-%m-%d')
        end_date = station_df['actual_time'].max().strftime('%Y-%m-%d')

        #get latitude and longitude for API call
        latitude = stations[station]['latitude']
        longitude = stations[station]['longitude']

        #call the api
        station_weather = openmeteo_api_call(start_date, end_date, station, latitude, longitude)

        print(f'{station} weather added')
        weather_dfs.append(station_weather)
        time.sleep(2)
    
    #join the weather dataframes
    joined_weather_df = pd.concat(weather_dfs, ignore_index=True)

    #save to csv and return
    filepath = RAW_DATA / f"{from_location}_{to_location}_weather.csv"
    joined_weather_df.to_csv(filepath, index=False)

    return joined_weather_df

def join_train_weather_data(stoppings_df, weather_df, from_location, to_location):
    """
    Merges a train dataframe and a weather dataframe based on the actual time (actual departure time unless terminus, in which case arrival time),
    rounded to the nearest hour and on the station code. The resulting data is saved as interim data.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station.
    - weather_df (dataframe): a dataframe of hourly weather for each location along the route.
    - from_location (str): the origin station code
    - to_location (str): the terminus station code

    Returns:
    - merged_df (dataframe): a dataframe where each row is a record of a train stopping at a station as well as the weather for the hour nearest this
    time
    """
    #round passing time to the nearest hour
    stoppings_df['nearest_hour'] = stoppings_df['actual_time'].dt.round('h')
    stoppings_df['nearest_hour'] = stoppings_df['nearest_hour'].dt.tz_localize('Europe/London', 
                                                                   nonexistent='NaT',
                                                                   ambiguous='NaT').dt.tz_convert('UTC') #set ambiguous or non-existent time to NaT (generally due to time zone changes)
    
    #drop train records within an hour of time changes to save confusion
    stoppings_df = stoppings_df.dropna(subset=['nearest_hour'])

    #merge with weahter on date, nearest hour and station
    merged_df = pd.merge(
        stoppings_df,
        weather_df,
        left_on=['nearest_hour', 'station'],
        right_on=['date', 'station'],
        how='left'
    )

    #drop na for all weather features
    merged_df.dropna(subset=NUMERICAL_FEATURES, inplace=True)
    merged_df.dropna(subset=['is_day'], inplace=True)

    #get filepath and save copy
    filepath = INDIVIDUAL_DIRECTIONS / f'{from_location}_{to_location}_final.csv'
    merged_df.to_csv(filepath, index=False)

    return merged_df

def concat_stoppings_dfs(stoppings_df_a, stoppings_df_b):
    """
    Joins two train dataframes(must be in the same format) together

    Args:
    - stoppings_df_a (dataframe): a dataframe where each row is a record of a train stopping at a station.
    - stoppings_df_a (dataframe): a dataframe in the same format as stoppings_df_a.

    Returns:
    - merged_df (dataframe): stoppings_df_a and stoppings_df_b merged into a single dataframe
    """
    df_list = [stoppings_df_a, stoppings_df_b]

    merged_df = pd.concat(df_list, ignore_index=True)

    return merged_df

