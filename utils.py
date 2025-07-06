import requests
import json
import base64  # Import base64 module for encoding
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd

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

    #if more than 12 hours early assume it crosses midnight and add 24 hours to the calculation
    if delay < -720:  
        delay += 1440 

    return delay

def calculate_delay_classification(delay_minutes):
    """
    """
    if delay_minutes < 5:
        return "Not Delayed"
    elif 5 <= delay_minutes < 15:
        return "Mild Delay"
    elif 15 <= delay_minutes < 30:
        return "Moderate Delay"
    elif delay_minutes >= 30:
        return "Severe Delay"
    elif pd.isna(delay_minutes):
        return "Issue Classifying"
    
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
