import pandas as pd
from utils import fetch_train_details
import time

#load the RIDs CSV
rids_df = pd.read_csv("EDA_EUS_TO_LIV_RIDS_2023-25.csv")

#placeholder
journey_detail_records = []

#fetch details for each RID

for index, row in rids_df.iterrows():
    rid = row["RID"]

    try:
        train_details = fetch_train_details(rid)
        if train_details:
            journey_detail_records.append(train_details)

        #delay to avoid overwhelming the API
        time.sleep(1)
    except Exception as e:
        print(f"Error fetching details for {rid}: {e}")


journey_details_df = pd.DataFrame(journey_detail_records)

#Mandatory first columns
mandatory_cols = ["RID" ,"Date", "TOC"]

core_stations = ["EUS", "WFJ", "MKC", "TAM", "LTV", "STA", "CRE", "RUN", "LIV"]
station_cols = []
for stn in core_stations:
    station_cols += [col for col in journey_details_df.columns if stn in col]


ordered_cols = mandatory_cols + station_cols
journey_details_df = journey_details_df[ordered_cols]

journey_details_df.to_csv("eus_liv_details_2023-25.csv", index=False)

print("Train service details saved successfully!")
print(journey_details_df.head())  # Preview first few rows



