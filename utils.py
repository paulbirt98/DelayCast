import requests
import json
import base64  # Import base64 module for encoding
import time
from datetime import datetime, timedelta

# HSP API URL for Service Metrics
url = "https://hsp-prod.rockshore.net/api/v1/serviceMetrics"

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
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=120)

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
                records.append({"Date": date, "Time Period": str(time_period), "RID": rid})

    except requests.exceptions.RequestException as e:
        print(f"API Request Error for {date} ({str(time_period)}): {e}")

    time.sleep(5)  # added to prevent overwhelming the API
    return records