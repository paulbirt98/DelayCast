import pandas as pd

train_df = pd.read_csv("eda_data_with_delays.csv")
weather_df = pd.read_csv("cre_eda_weather.csv")

# Ensure datetime columns are parsed correctly
train_df['EUS Scheduled Departure Time'] = pd.to_datetime(train_df['EUS Scheduled Departure Time'])
weather_df['date'] = pd.to_datetime(weather_df['date'], utc=True)  # force UTC timezone awareness

# Convert train_df departure_hour to UTC to match weather_df['date']
train_df['departure_hour'] = train_df['EUS Scheduled Departure Time'].dt.round('h').dt.tz_localize('Europe/London').dt.tz_convert('UTC')

# Merge with weather data on datetime
merged_df = train_df.merge(
    weather_df,
    left_on='departure_hour',
    right_on='date',
    how='left'
)

# Check result
print(merged_df.head())

merged_df.to_csv("cre_merged_eda_data.csv")
