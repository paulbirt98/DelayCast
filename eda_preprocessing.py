import pandas as pd

#load the data to a dataframe
service_details_df = pd.read_csv("eus_liv_details_2023-25.csv")

#drop rows with missing data at the key stations to be examined
processed_service_details_df = service_details_df.dropna(subset=['EUS Actual Departure Time', 
                                                                 'MKC Actual Departure Time',
                                                                 'CRE Actual Departure Time',
                                                                 'LIV Actual Arrival Time'])

#check that the number of rows has decreased
print(len(service_details_df))
print(len(processed_service_details_df))

