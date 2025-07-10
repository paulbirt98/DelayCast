import pandas as pd
from sklearn.feature_selection import SelectKBest, f_classif

# Load your data
df = pd.read_csv('../eus_liv_joined.csv')

# Define numeric features and target
X = df[['temperature_2m', 'snowfall', 'snow_depth', 'rain','apparent_temperature', 'cloud_cover', 'wind_speed_10m', 'wind_gusts_10m']]  # replace with your numeric columns
y = df['Delay Classification']  # categorical target

# Apply ANOVA F-test
selector = SelectKBest(score_func=f_classif, k='all')
selector.fit(X, y)

# Output results
anova_results = pd.DataFrame({
    'Feature': X.columns,
    'F_score': selector.scores_
}).sort_values(by='F_score', ascending=False)

print("ANOVA F-test scores for numeric features vs delay classification:")
print(anova_results)

# Optional: Save to CSV
anova_results.to_csv('anova_f_scores.csv', index=False)
