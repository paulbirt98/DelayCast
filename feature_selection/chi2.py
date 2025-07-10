import pandas as pd
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.preprocessing import LabelEncoder

df = pd.read_csv('../eus_liv_joined.csv')

#define features and target to be checked against
x = df[['is_day', 'weather_code', 'Year', 'Month', 'Day', 'Hour',]]
y = df['Delay Classification']

#features to integers
x_encoded = x.apply(LabelEncoder().fit_transform)

#encode target to integers
y_encoded = LabelEncoder().fit_transform(y)

#test
selector = SelectKBest(score_func=chi2, k='all')
selector.fit(x_encoded, y_encoded)

#output results
print('Chi-square test scores for categorical features')
for feature, score in zip(x.columns, selector.scores_):
    print(f"{feature}: Chi-square score = {score:.2f}")


