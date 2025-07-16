import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFE
from sklearn.preprocessing import LabelEncoder

df = pd.read_csv('../data/processed/eus_liv_joined.csv')

X = df[['Hour', 'Month', 'is_day', 'temperature_2m', 'rain', 'snow_depth', 'wind_speed_10m']]  # use your selected features
y = LabelEncoder().fit_transform(df['Delay Classification'])  # encode target

model = RandomForestClassifier()
selector = RFE(model, n_features_to_select=5)
selector = selector.fit(X, y)

print(pd.Series(selector.support_, index=X.columns))  # True = selected
