import pandas as pd

train_df = pd.read_csv("eus_liv_w_derived_features.csv")
weather_df = pd.read_csv("eus_liv_weather.csv")

# Ensure datetime columns are parsed correctly
train_df['Actual Time'] = pd.to_datetime(train_df['Actual Time'])
weather_df['date'] = pd.to_datetime(weather_df['date'])

#round train passing to nearest hour
train_df['Nearest Hour'] = train_df['Actual Time'].dt.round('h')
train_df['Nearest Hour'] = train_df['Nearest Hour'].dt.tz_localize('Europe/London', 
                                                                   nonexistent='NaT',
                                                                   ambiguous='NaT').dt.tz_convert('UTC')

#drop trains within an hour of time zone changes
train_df = train_df.dropna(subset=['Nearest Hour'])


# Merge with weather data on datetime 
merged_df = pd.merge(
    train_df,
    weather_df,
    left_on=['Nearest Hour', 'Station'],
    right_on=['date', 'Station'],
    how='left'
)

merged_df.to_csv("eus_liv_joined.csv", index=False)
