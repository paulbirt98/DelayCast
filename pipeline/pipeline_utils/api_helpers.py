import requests
import json
from pipeline_utils.config import HEADERS, METRICS_URL, DETAILS_URL, FROM_TIME, TO_TIME, MAX_WORKERS
from datetime import datetime, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

#Function to fetch all the RIDs for trains within a given date range
def call_service_metrics_api(payload, date, time_period):
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
        response = requests.post(METRICS_URL, headers=HEADERS, data=json.dumps(payload), timeout=120)

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
                records.append({"rid": rid, "date": date, "time_period": str(time_period)})

    except requests.exceptions.RequestException as e:
        print(f"API Request Error for {date} ({str(time_period)}): {e}")
        return [] 
    except Exception as e:
        print(f"Unexpected error for {date} ({time_period}): {e}")
        return [] 

    return records

def fetch_rids(from_location, to_location, atoc, from_date, to_date, testing=False, max_workers=MAX_WORKERS):
    """
    A function to loop through each date between from_date and to_date (inclusive), and through each hour slot between the given 
    FROM_TIME and TO_TIME (inclusive) as set in config.py

    Arguments:
    - from_location (str): The origin station CRS code
    - to_location (str): The terminus station CRS code
    - from_date (datetime): The first date in the desired date range 
    - to_date (datetime): The last date in the desired date range

    Returns:
    - rid_records_df (dataframe): A dataframe of all RIDs fetched for all trains fitting the API request parameters within the given date range.
    Includes columns 'rid', 'date', and 'time_period'
     
    """
    # Generate a list of all dates within that range
    date_list = [(from_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((to_date - from_date).days + 1)]

    #if in test mode only gather the most recent 3 days data to check formatting etc
    if testing:
        date_list = list(reversed(date_list))[:3]

    # Placeholder for the dataset returned
    rid_records = []

    #placeholder for the arguments needed for API call
    api_args = []

    # Loop through each date
    for date in date_list:
        
        #get the day of week as an integer and assign appropriately
        day_index = datetime.strptime(date, "%Y-%m-%d").weekday()
        days = "WEEKDAY" if day_index <= 4 else "SATURDAY" if day_index == 5 else "SUNDAY"

        for hour_slot_start in range(FROM_TIME, TO_TIME): #loop through each time slot from 6 (06:00) to 21 (21:00)

            hour_slot_end = hour_slot_start + 1

            #define the data to be fetched in each call
            payload = {
                "from_loc": from_location,
                "to_loc": to_location,
                "from_time": f"{hour_slot_start:02d}00",
                "to_time": f"{hour_slot_end:02d}00",
                "from_date": date, 
                "to_date": date,
                "days": days, 
                "toc_filter": [atoc]
            }
            
            api_args.append((payload, date, hour_slot_start))
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        #list of future objects with api responses
        futures =  [
            executor.submit(call_service_metrics_api, payload, date, hour_slot_start)
            for payload, date, hour_slot_start in api_args
            ]
        
        #iterate over futures as each call is completed
        for future in as_completed(futures):
            #if data is retrieved add it to rid_records
            try:
                api_result = future.result()
                if api_result:
                    rid_records.extend(api_result)
            except Exception as e:
                print(f"Error in fetching RID via thread: {e}")
    
    #convert to df
    rid_records_df = pd.DataFrame(rid_records)

    return rid_records_df

def call_service_details_api(rid, avoid=None):
    """
    Fetch service details from the HSP API for a given RID.

    Args:
    - rid (str): The service RID.
    - avoid (str): Optional. A three letter station code, if present in the list of stops returned by the API this journey will be skipped
    and will not be saved.

    Returns:
    - dict: The fetched train journey details, including all scheduled and actual departure times, except for the final station for which
    the scheduled and actual arrival times are given.
    """
    try:
        response = requests.post(DETAILS_URL, headers=HEADERS, json={"rid": rid}, timeout=30)

        if response.status_code == 502:
            print(f"502 Bad Gateway error on {rid}.")
            return None

        response.raise_for_status()
        data = response.json()

        # Extract relevant train details
        details = data.get("serviceAttributesDetails", {})
        date_of_service = details.get("date_of_service", "")
        stops = details.get("locations", [])

        # Create a structured dictionary for the journey
        journey_record = {
            "date": date_of_service,
            "rid": rid,
            "toc": details.get("toc_code", ""),
        }

        # Extract stop-wise data (unless the 'avoid' station is present in stations list returned)
        for i, stop in enumerate(stops):
            if avoid in stops:
                continue

            station = stop.get("location", "").lower()

            if i < (len(stops) - 1):  # Get departure times for all but final station
                if stop.get("gbtt_ptd"):
                    journey_record[f"{station}_scheduled_departure_time"] = stop.get("gbtt_ptd")
                if stop.get("actual_td"):
                    journey_record[f"{station}_actual_departure_time"] = stop.get("actual_td")
            
            if i == (len(stops) - 1): # Get arrival times for the final station
                if stop.get("gbtt_pta"):
                    journey_record[f"{station}_scheduled_arrival_time"] = stop.get("gbtt_pta")
                if stop.get("actual_ta"):
                    journey_record[f"{station}_actual_arrival_time"] = stop.get("actual_ta")

            journey_record[f"{station}_lc_reason"] = stop.get("late_canc_reason", "")
        
        # Print success message
        print(f"Success for {rid} - {date_of_service}")
        return journey_record

    except requests.exceptions.RequestException as e:
        print(f"API Request Error for RID {rid}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected Error for {rid}: {e}")
        return None
    
def fetch_train_times(rids_df, avoid=None, max_workers=MAX_WORKERS):
    """
    A function to loop through each RID in an RID dataframe and call the HSP Service Details API

    Arguments:
    - rids_df (dataframe): A dataframe containing RIDs for each train journey from which service details are desired
    - avoid (str): Optional. A three letter station code, if present in the list of stops returned by the API this journey will be skipped
    and will not be saved.
    - max_workers (int): the number of concurrent threads, set in config.py

    Returns:
    - journey_details_df (dataframe): A dataframe of all journey details for all given RIDs
    """
    #placeholder
    journey_detail_records = []
    rids = rids_df['rid']

    #create a thread pool for up to 10 concurrent calls
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_rid = {executor.submit(call_service_details_api, rid, avoid): rid for rid in rids}

        for future in as_completed(future_rid):
            rid = future_rid[future]
            try:
                result = future.result()
                if result:
                    journey_detail_records.append(result)
            except Exception as e:
                print(f"Error fetching details for {rid}: {e}")


    journey_details_df = pd.DataFrame(journey_detail_records)

    #Mandatory first columns
    mandatory_cols = ["rid" ,"date", "toc"]

    # Get all columns except mandatory_cols to preserve their existing order
    other_cols = [col for col in journey_details_df.columns if col not in mandatory_cols]

    # Combine
    ordered_cols = mandatory_cols + other_cols

    # Reorder DataFrame without excluding non-core stations
    journey_details_df = journey_details_df[ordered_cols]

    return journey_details_df