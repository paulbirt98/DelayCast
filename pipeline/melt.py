import pandas as pd
from scripts.transform import core_stations

cut_service_details = pd.read_csv("eus_liv_2015-25_cut.csv", dtype=str)

id_variables = ['RID', 'Date', 'TOC']
dataframes =  []

for i, station in enumerate(core_stations):

    #assign station specific columns to appropriate variables
    if i < (len(core_stations) - 1):
        scheduled = f"{station} Scheduled Departure Time"
        actual = f"{station} Actual Departure Time"
    else:
        scheduled = f"{station} Scheduled Arrival Time"
        actual = f"{station} Actual Arrival Time"
    
    reason_code = f"{station} LC Reason"

    #check these column names exist in the df and create a list
    station_columns = [col for col in [scheduled, actual, reason_code] if col in cut_service_details.columns]

    #create a df for each station to be joined up later
    station_df = cut_service_details[id_variables + station_columns].copy()
    #extract station code for use as a feature
    station_df['Station'] = station

    #rename time columns for uniformity
    if scheduled in station_df.columns:
        station_df.rename(columns={scheduled: "Scheduled Time"}, inplace=True)
    if actual in station_df.columns:
        station_df.rename(columns={actual: "Actual Time"}, inplace=True)
    
    station_df.rename(columns={reason_code: "Reason Code"}, inplace=True)

    #append to df list
    dataframes.append(station_df)
    
#join all the dataframes in the list together
joined_dataframe = pd.concat(dataframes, ignore_index=True)

joined_dataframe = joined_dataframe[['RID', 'Date', 'TOC', 'Station', 'Scheduled Time', 'Actual Time', 'Reason Code']]

joined_dataframe.to_csv("eus_liv_long_format.csv", index=False)