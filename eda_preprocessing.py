import pandas as pd
from utils import convert_to_datetime, calculate_delay, calculate_delay_classification

#load the data to a dataframe
service_details_df = pd.read_csv("eus_liv_details_2023-25.csv")

#drop rows with missing data at the key stations to be examined
processed_service_details_df = service_details_df.dropna(subset=['EUS Actual Departure Time', 
                                                                 'MKC Actual Departure Time',
                                                                 'CRE Actual Departure Time',
                                                                 'LIV Actual Arrival Time']).copy()

print(processed_service_details_df.head(5))

#check that the number of rows has decreased
print(len(service_details_df))
print(len(processed_service_details_df))

#convert the time columsn to datetime
time_columns = [col for col in processed_service_details_df.columns if 'Arrival Time' in col or 'Departure Time' in col]

for col in time_columns:
    processed_service_details_df[col] = processed_service_details_df.apply(
        lambda row: convert_to_datetime(row['Date'], row[col]) if pd.notnull(row[col]) else pd.NaT,
        axis=1
    )

print(processed_service_details_df.head(5))

#calculate the delays at each station
stations = ['EUS', 'MKC', 'CRE', 'LIV']

for i, station in enumerate(stations):
    if i < (len(stations) -1):
        scheduled_col = f"{station} Scheduled Departure Time"
        actual_col = f"{station} Actual Departure Time"
    else:
        scheduled_col = f"{station} Scheduled Arrival Time"
        actual_col = f"{station} Actual Arrival Time"

    processed_service_details_df[f"{station} Delay Minutes"] = processed_service_details_df.apply(
        lambda row: calculate_delay(row[scheduled_col], row[actual_col]), axis=1
    )

    processed_service_details_df[f"{station} Delay Classification"] = processed_service_details_df[f"{station} Delay Minutes"].apply(calculate_delay_classification)

print(processed_service_details_df.columns[-10:])  # Show last few columns


processed_service_details_df.to_csv("eda_data_with_delays.csv", index=False)