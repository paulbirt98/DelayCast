import pandas as pd
from transform import core_stations

eus_liv_cleaned = pd.read_csv("eus_liv_long_format.csv").copy()

print(eus_liv_cleaned.shape)

eus_liv_cleaned['Scheduled Time'] = pd.to_datetime(eus_liv_cleaned['Scheduled Time'], errors='coerce')
eus_liv_cleaned['Actual Time'] = pd.to_datetime(eus_liv_cleaned['Actual Time'], errors='coerce')

eus_liv_cleaned['Reason Code'] = eus_liv_cleaned['Reason Code'].fillna('000').astype(str)
eus_liv_cleaned['RID'] = eus_liv_cleaned['RID'].astype('Int64')

eus_liv_cleaned['Date'] = eus_liv_cleaned['Date'].astype(str)
eus_liv_cleaned['Station'] = eus_liv_cleaned['Station'].astype(str)
eus_liv_cleaned['TOC'] = eus_liv_cleaned['TOC'].astype(str)

eus_liv_cleaned.dropna(subset=["TOC", "Station", "Scheduled Time", "Actual Time"], inplace=True)

print(eus_liv_cleaned.shape)


eus_liv_cleaned.to_csv("eus_liv_cleaned.csv", index=False)