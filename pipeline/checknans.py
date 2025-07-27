import pandas as pd
from pipeline_utils.config import NUMERICAL_FEATURES, INDIVIDUAL_ROUTES

df = pd.read_csv(INDIVIDUAL_ROUTES / 'btn_bdm_route.csv')

# Drop any rows with at least one NaN
df_clean = df.dropna()

# Save cleaned CSV
df_clean.to_csv(INDIVIDUAL_ROUTES / 'btn_bdm_route.csv', index=False)

print(f"Dropped {len(df) - len(df_clean)} rows with NaNs.")