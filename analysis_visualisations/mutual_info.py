import pandas as pd
from sklearn.feature_selection import mutual_info_classif
from sklearn.preprocessing import LabelEncoder

df = pd.read_csv('../data/processed/eus_liv_joined.csv')

numerical_features = df[['temperature_2m', 'snowfall', 'snow_depth', 'rain','apparent_temperature', 'cloud_cover', 'wind_speed_10m', 'wind_gusts_10m']]
#encode categorical features
categorical_features = df[['is_day', 'weather_code', 'Year', 'Month', 'Day', 'Hour']]
cat_features_encoded = categorical_features.apply(LabelEncoder().fit_transform)

all_features = pd.concat([numerical_features, cat_features_encoded], axis=1)

target = df['Delay Classification']

scores = mutual_info_classif(all_features, target, discrete_features=False)
scores_w_features = pd.Series(scores, index=all_features.columns).sort_values(ascending=False)

print(scores_w_features)