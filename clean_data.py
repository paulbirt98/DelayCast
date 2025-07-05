import pandas as pd
from utils import convert_to_datetime, calculate_delay, calculate_delay_classification

#load the data to a dataframe
service_details_df = pd.read_csv("eus_liv_details_2015-25.csv")

#drop rows with missing data at the key stations to be examined
processed_service_details_df = service_details_df.copy()

#trim the data to only include target / core stations
mandatory_columns = ['RID', 'Date', 'TOC']
core_stations = ['EUS', 'WFJ', 'MKC', 'RUG', 'NUN', 'TAM', 'LTV', 'STA', 'CRE', 'RUN', 'LIV']
station_columns = []

for i, station in enumerate(core_stations):
    if i < (len(core_stations)-1):
        cols = [col for col in processed_service_details_df.columns if station in col]
    else: 
        cols = [col for col in processed_service_details_df.columns if station in col and 'Departure' not in col]
    station_columns.extend(cols)


ordered_columns = mandatory_columns + station_columns

try:
    processed_service_details_df = processed_service_details_df[ordered_columns].copy()
except KeyError as e:
    print("KeyError:", e)

#convert the time columsn to datetime
time_columns = [col for col in processed_service_details_df.columns if 'Arrival Time' in col or 'Departure Time' in col]

for col in time_columns:
    processed_service_details_df[col] = processed_service_details_df.apply(
        lambda row: convert_to_datetime(row['Date'], row[col]) if pd.notnull(row[col]) else pd.NaT,
        axis=1
    )

print(processed_service_details_df.shape)

for i, station in enumerate(core_stations):
    if i < (len(core_stations) - 1):
        scheduled = f"{station} Scheduled Departure Time"
        actual = f"{station} Actual Departure Time"
    else:
        scheduled = f"{station} Scheduled Arrival Time"
        actual = f"{station} Actual Arrival Time"

    condition = processed_service_details_df[scheduled].notnull() & processed_service_details_df[actual].isnull()
    processed_service_details_df = processed_service_details_df[~condition]


print(processed_service_details_df.shape)

processed_service_details_df.to_csv("eus_liv_2015-25_cleaned.csv", index=False)