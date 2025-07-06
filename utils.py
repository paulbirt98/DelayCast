import requests
import json
import base64  # Import base64 module for encoding
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

# HSP API URL for Service Metrics
metrics_url = "https://hsp-prod.rockshore.net/api/v1/serviceMetrics"
details_url = "https://hsp-prod.rockshore.net/api/v1/serviceDetails"

# My National Rail Data login details
email = "paulbirt1998@gmail.com"
password = "q1XHAkT69ywqxd!"

# Encode email & password for Basic Authentication
auth_string = f"{email}:{password}"
auth_header = base64.b64encode(auth_string.encode()).decode()

# Define headers with Basic Auth
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {auth_header}"
}

#Function to fetch all the RIDs for trains within a given date range
def fetch_rids(payload, date, time_period):
    """
    Fetch RIDs from the HSP API for a given date and time period.

    Arguments:
    - payload (dict): API request parameters.
    - date (str): The date of the request.
    - time_period (int): number to correspond to the hour of the day i.e. 1 = trains from 01:00 to 02:00 etc.
                        throws an exception if an invalid number (< 0 or > 23 is passed)

    Returns:
    - list of dictionaries: RIDs with their respective date and time period.
    """
    if (time_period < 0 or time_period > 23):
            raise ValueError("Time period must be an integer between 0 and 23 inclusive")

    records = []

    try:
        response = requests.post(metrics_url, headers=headers, data=json.dumps(payload), timeout=120)

        if response.status_code == 502:
            print(f"502 Bad Gateway error on {date} ({str(time_period)}).")
            return [] 

        #if enother error then send to exception block
        response.raise_for_status()

        #assigns the response to a variable called data
        data = response.json()

        # Print success message for tracking
        print(f"RID for {date} ({str(time_period)}) fetched successfully")

        # Extract relevant service data
        services = data.get("Services", [])
        if not services:
            print(f"No services found for {date} ({str(time_period)}).")
            return []

        for service in services:
            rids = service.get("serviceAttributesMetrics", {}).get("rids", [])
            for rid in rids:
                records.append({"RID": rid, "Date": date, "Time Period": str(time_period)})

    except requests.exceptions.RequestException as e:
        print(f"API Request Error for {date} ({str(time_period)}): {e}")

    time.sleep(1)  # added to prevent overwhelming the API
    return records

#Function to fetch all details for a train journey given the RID
def fetch_train_details(rid):
    """
    Fetch service details from the HSP API for a given RID.

    Args:
    - rid (str): The service RID.

    Returns:
    - dict: Extracted train journey details.
    """
    try:
        response = requests.post(details_url, headers=headers, json={"rid": rid}, timeout=30)

        if response.status_code == 502:
            print(f"502 Bad Gateway error on {rid}.")
            return None

        response.raise_for_status()
        data = response.json()

        # Extract relevant train details
        details = data.get("serviceAttributesDetails", {})
        date_of_service = details.get("date_of_service", "-")
        stops = details.get("locations", [])

        # Create a structured dictionary for the journey
        journey_record = {
            "Date": date_of_service,
            "RID": rid,
            "TOC": details.get("toc_code", "-"),
        }

        # Extract stop-wise data
        for i, stop in enumerate(stops):
            station = stop.get("location", "-")
    
            if i > 0:  # Skip arrival times for first station
                journey_record[f"{station} Scheduled Arrival Time"] = stop.get("gbtt_pta", "-")
                journey_record[f"{station} Actual Arrival Time"] = stop.get("actual_ta", "-")

            if i < (len(stops) - 1):  # Skip departure times for last station
                journey_record[f"{station} Scheduled Departure Time"] = stop.get("gbtt_ptd", "-")
                journey_record[f"{station} Actual Departure Time"] = stop.get("actual_td", "-")

            journey_record[f"{station} LC Reason"] = stop.get("late_canc_reason", "-")
        
        # Print success message
        print(f"Success for {rid} - {date_of_service}")
        return journey_record

    except requests.exceptions.RequestException as e:
        print(f"API Request Error for RID {rid}, {date_of_service}: {e}")
        return None
    
def convert_to_datetime(date, time):
    """
    Combines a date and time to form a datetime object

    Args:
    date and time(float) to be combined

    Returns:
    a datetime object
    """
    return pd.to_datetime(f"{date} {int(time):04}", format="%Y-%m-%d %H%M")

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
    if delay < -720 or delay > 720:  
        delay = None

    return delay

def calculate_delay_classification(delay_minutes):
    """
    """
    if pd.isna(delay_minutes):
        return "Issue Classifying"
    elif delay_minutes < 5:
        return "Not Delayed"
    elif 5 <= delay_minutes < 15:
        return "Mild Delay"
    elif 15 <= delay_minutes < 30:
        return "Moderate Delay"
    elif delay_minutes >= 30:
        return "Severe Delay"
    
def cut_non_core_stations(dataframe, core_stations):
    """
    """
    mandatory_columns = ['RID', 'Date', 'TOC']
    station_columns = []

    for i, station in enumerate(core_stations):
        if i < (len(core_stations)-1):
            cols = [col for col in dataframe.columns if station in col]
        else: 
            cols = [col for col in dataframe.columns if station in col and 'Departure' not in col]
        station_columns.extend(cols)

    ordered_columns = mandatory_columns + station_columns
    return ordered_columns

def weather_call(start_date, end_date, latitude, longitude, station):
    """
    """
    #variable to handle rate limit exceedance
    rate_limit_message = "Minutely API request limit exceeded"
    wait_time = 60
    retries = 5

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
        "hourly": ["temperature_2m", "snowfall", "snow_depth", "rain", "precipitation", "apparent_temperature", "weather_code", "is_day", "cloud_cover", "relative_humidity_2m", "wind_speed_10m", "wind_gusts_10m"],
        "timezone": "Europe/London"
    }

    for tries in range(retries):
        try:
            responses = openmeteo.weather_api(url, params=params)
            break
        except Exception as e:
            print(f"Request failed: {e}")
            if rate_limit_message in str(e):
                print(f"Rate limit exceeded, waiting {wait_time} before retrying")
                time.sleep(wait_time)
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
    hourly_snowfall = hourly.Variables(1).ValuesAsNumpy()
    hourly_snow_depth = hourly.Variables(2).ValuesAsNumpy()
    hourly_rain = hourly.Variables(3).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(5).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(6).ValuesAsNumpy()
    hourly_is_day = hourly.Variables(7).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(8).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(9).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(10).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(11).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["snow_depth"] = hourly_snow_depth
    hourly_data["rain"] = hourly_rain
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["weather_code"] = hourly_weather_code
    hourly_data["is_day"] = hourly_is_day
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m

    hourly_dataframe = pd.DataFrame(data = hourly_data)

    #add column for station code
    hourly_dataframe['Station'] = station

    return hourly_dataframe
