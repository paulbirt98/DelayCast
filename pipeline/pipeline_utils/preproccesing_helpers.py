import pandas as pd
from pipeline_utils.config import FREQ_THRESHOLD, RECORDING_ERROR_MIN, RECORDING_ERROR_MAX, NO_DELAY_UPPER_BOUNDARY, MILD_DELAY_UPPER_BOUNDARY, MODERATE_DELAY_UPPER_BOUNDARY
import json
from pathlib import Path

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
        journeys_df[col] = journeys_df.apply(
            lambda row: pd.to_datetime(f"{row['date']} {int(row[col]):04}", format="%Y-%m-%d %H%M") if pd.notnull(row[col]) else pd.NaT,
            axis=1
        )

    print("Converted to datetime")

    return journeys_df

def drop_low_freq_stations(journeys_df, from_location, to_location):
    """
    Removes any column relating to stations which appear less frequently (as a percentage fo total rows in the passed dataframe)
    than FREQ_THRESHOLD as set in config.py. Also saves a list of all station codes that do pass the threshold to a json file in 
    data/semi_processed.

    Args:
    - journeys_df (dataframe): a dataframe containing details of all train journeys on a given route
    - from_location (str): the origin station code, used in naming the json file
    - to_location (str): the terminus station code, used in naming the json file

    Returns:
    - journeys_df (dataframe): the same dataframe passed in as an argument but with all low frequency stations removed
    """
    
    #a set of unique station codes
    unique_station_codes = set(
        [col.split('_')[0] for col in journeys_df.columns if '_scheduled_departure_time' in col]
    )

    #drop any stations that appear less than 5% of the time
    for station in unique_station_codes.copy():
        station_columns = [col for col in journeys_df.columns if station in col]
        station_scheduled_column = f"{station}_scheduled_departure_time"
        if journeys_df[station_scheduled_column].count() < (len(journeys_df) * FREQ_THRESHOLD):
            print(f"Dropping {station} from the dataframe due to low frequency")
            journeys_df.drop(columns=station_columns, inplace=True)
            unique_station_codes.discard(station)
    
    #convert unique codes to a list and then save to a json file
    unique_station_codes = list(unique_station_codes)

    with open(f"{from_location}_{to_location}_station_codes.json", "w") as json_file:
        json.dump(unique_station_codes, json_file)

    return journeys_df

def load_json_to_list(from_location, to_location):
    """
    Loads the relevant json file to the script containing stations on a route, given the from_location and to_location passed in.

    Args:
    - from_location (str): the origin station code
    - to_location (str): the terminus station code

    Returns:
    - station_codes (list[str]): A list of all saved station codes on a route
    """
    with open(f"{from_location}_{to_location}_station_codes", "r") as file:
        station_codes = json.load(file)
    
    return station_codes

def to_long_format(journeys_df, station_codes):
    """
    Converts a dataframe of train journeys to long format. I.e. each row will now relate to a a single train passing a single station,
    rather than a row relating to an entire route.

    Args:
    - journeys_df (dataframe): a dataframe containing details of all train journeys on a given route
    - station_codes (list[str]): A list of strings containing the three letter CRS code identifying all stations

    Returns:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station
    """
    #define id variables, to stay as columns
    id_variables = ['rid', 'date', 'toc']
    #placeholder list for dataframes
    dataframes = []

    print("Converting to long format")

    for i, station in enumerate(station_codes):

        print(f"Station: {station}")
        
        #assign station specific columns to general scheduled vs actual variable
        if i < (len(station_codes) -  1):
            scheduled = f"{station}_scheduled_departure_time"
            actual = f"{station}_actual_departure_time"
        else:
            scheduled = f"{station}_scheduled_arrival_time"
            actual = f"{station}_actual_arrival_time"

        reason_code = f"{station}_lc_reason"

        #check these columns exist and create a list
        station_columns = [col for col in [scheduled, actual, reason_code] if col in journeys_df.columns]

        if scheduled in station_columns:
            print(f"{station} exists in dataframe")
        else:
            print(f"Station {station} wasnt found in the dataframe")

        #create a dataframe for each station to be concatenated later
        stations_df = journeys_df[id_variables + station_columns].copy()
        #make station its own column
        stations_df['station'] = station

        #rename time columns for uniformity
        if scheduled in stations_df.columns:
            stations_df.rename(columns={scheduled: "scheduled_time"}, inplace=True)
        if actual in stations_df.columns:
            stations_df.rename(columns={actual: "actual_time"}, inplace=True)
        
        stations_df.rename(columns={reason_code: "lc_reason"}, inplace=True)

        #append to the dataframe list
        dataframes.append(stations_df)

    #join all dataframes back together
    stoppings_df = pd.concat(dataframes, ignore_index=True)
    stoppings_df = stoppings_df[['rid', 'date', 'toc', 'station', 'scheduled_time', 'actual_time', 'lc_reason']]

    return stoppings_df

def clean(stoppings_df):
    """
    Cleans a dataframe of train stoppings by enforcing the datatype of each feature as well as removing any rows with missing values 
    for TOC, Station, Scheduled Time or Actual Time. Also fills any rows with no late_cancellation reason code with 000 to represent none.

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station

    Returns:
    - stoppings_df (dataframe): the same dataframe as was passed in but with missing values removed
    """
    print(f"Cleaning dataframe, shape before: {stoppings_df.shape}")

    stoppings_df['scheduled_time'] = pd.to_datetime(stoppings_df['scheduled_time'], errors='coerce')
    stoppings_df['actual_time'] = pd.to_datetime(stoppings_df['actual_time'], errors='coerce')

    stoppings_df['lc_reason'] = stoppings_df['lc_reason'].fillna('000').astype(str)
    stoppings_df['rid'] = stoppings_df['rid'].astype('Int64')

    stoppings_df['date'] = stoppings_df['date'].astype(str)
    stoppings_df['station'] = stoppings_df['station'].astype(str)
    stoppings_df['toc'] = stoppings_df['toc'].astype(str)

    stoppings_df.dropna(subset=['toc', 'station', 'scheduled_time', 'actual_time'], inplace=True)

    print(f"Shape after: {stoppings_df.shape}")

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

def get_direction_feature(stoppings_df, from_location, to_location):
    """
    Add direction as a feature, example format for a EUS to LIV service: 'EUS-LIV'

    Args:
    - stoppings_df (dataframe): a long format dataframe where each row is a record of a train stopping at a station
    - from_location (str): the origin station code
    - to_location (str): the terminus station code


    Returns:
    - stoppings_df (dataframe): the same dataframe as was passed in but with an added 'direction' feature  
    """
    stoppings_df['direction'] = f"{from_location}-{to_location}"

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
    elif NO_DELAY_UPPER_BOUNDARY <= delay_minutes < MILD_DELAY_UPPER_BOUNDARY:
        return "Mild Delay"
    elif MILD_DELAY_UPPER_BOUNDARY <= delay_minutes < MODERATE_DELAY_UPPER_BOUNDARY:
        return "Moderate Delay"
    elif delay_minutes >= MODERATE_DELAY_UPPER_BOUNDARY:
        return "Severe Delay"
    
