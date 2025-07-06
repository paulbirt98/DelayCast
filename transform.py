import pandas as pd
from utils import convert_to_datetime, cut_non_core_stations, calculate_delay, calculate_delay_classification

#load the data to a dataframe
service_details_df = pd.read_csv("eus_liv_details_2015-25.csv")

#trim the data to only include target / core stations
core_stations = ['EUS', 'WFJ', 'MKC', 'RUG', 'NUN', 'TAM', 'LTV', 'STA', 'CRE', 'RUN', 'LIV']

try:
    processed_service_details_df = service_details_df[
        cut_non_core_stations(service_details_df, core_stations)
    ].copy()
except KeyError as e:
    print("KeyError:", e)

#convert the time columsn to datetime
time_columns = [col for col in processed_service_details_df.columns if 'Arrival Time' in col or 'Departure Time' in col]

for col in time_columns:
    processed_service_details_df[col] = processed_service_details_df.apply(
        lambda row: convert_to_datetime(row['Date'], row[col]) if pd.notnull(row[col]) else pd.NaT,
        axis=1
    )

# scheduled arrival times and drop
scheduled_arrivals = [
    f"{station} Scheduled Arrival Time"
    for station in core_stations[:-1]  # Exclude the last station
]

processed_service_details_df = processed_service_details_df.drop(
    columns=[col for col in scheduled_arrivals if col in processed_service_details_df.columns]
)

# actual arrival times and drop apart from last
actual_arrivals = [
    f"{station} Actual Arrival Time"
    for station in core_stations[:-1]  # Exclude the last station
]

# Drop them if they exist
processed_service_details_df = processed_service_details_df.drop(
    columns=[col for col in actual_arrivals if col in processed_service_details_df.columns]
)

processed_service_details_df.to_csv("eus_liv_2015-25_cut.csv", index=False)