from utils import fetch_rids
import pandas as pd
from datetime import datetime, timedelta

callLimit = 20
callCount = 0

# Define start and end dates
start_date = datetime(2023, 6, 1)
end_date = datetime(2025, 5, 31) 

# Generate a list all dates within range
date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]

# Placeholder for the dataset returned
rid_records = []

# Loop through each date
for date in date_list:
    
    day_index = datetime.strptime(date, "%Y-%m-%d").weekday()
    days = "WEEKDAY" if day_index <= 4 else "SATURDAY" if day_index == 5 else "SUNDAY"

    for time_period_start in range(6, 22): #loop through each time slot from 6 (06:00) to 21 (21:00)

        if callCount >= callLimit:
            break

        time_period_end = time_period_start + 1

        #define the data to be fetched in each call
        payload = {
            "from_loc": "EUS",
            "to_loc": "LIV",
            "from_time": f"{time_period_start:02d}00",
            "to_time": f"{time_period_end:02d}00",
            "from_date": date,  # Assign the correct date dynamically
            "to_date": date,
            "days": days,  # Assign the correct day dynamically
            "toc_filter": ["VT"], 
        }

        print(payload)
        rid_records += fetch_rids(payload, date, time_period_start)
        callCount += 1

    if callCount >= callLimit:
        break
        
# Convert to Pandas DataFrame
df = pd.DataFrame(rid_records)

# Save data to CSV
df.to_csv("EDA_EUS_TO_LIV_RIDS_2023-25.csv", index=False)