import pandas as pd
from utils import calculate_delay, calculate_delay_classification

df = pd.read_csv("eus_liv_cleaned.csv", dtype={
    'RID': 'Int64',
    'Date': str,
    'TOC': str,
    'Station': str,
    'Reason Code': str
}, parse_dates=['Scheduled Time', 'Actual Time'])

#calculate delay minutes
df['Delay Minutes'] = df.apply(
    lambda row: calculate_delay(row['Scheduled Time'], row['Actual Time']), axis=1
)

#classify based on minutes
df['Delay Classification'] = df.apply(
    lambda row: calculate_delay_classification(row['Delay Minutes']), axis=1
)

#derive temporal features
df['Hour'] = df['Scheduled Time'].dt.hour
df['Day'] = df['Scheduled Time'].dt.dayofweek
df['Month'] = df['Scheduled Time'].dt.month
df['Year'] = df['Scheduled Time'].dt.year

df = df.drop(columns=['Unnamed: 0'])

##this needs moved to the cleaning script
df = df[df['Delay Classification'] != 'Issue Classifying']

df.to_csv("eus_liv_w_derived_features.csv", index=False)