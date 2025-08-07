import pandas as pd
from pipeline_utils.config import NUMERICAL_FEATURES, INDIVIDUAL_ROUTES, INDIVIDUAL_DIRECTIONS, UNIFIED_ROUTES_DIR, UNIFIED_ROUTES_FILE

route = 'liv_eus'

df = pd.read_csv(UNIFIED_ROUTES_FILE)

# Drop any rows with at least one NaN
df_clean = df.dropna()

# Save cleaned CSV
df_clean.to_csv(UNIFIED_ROUTES_FILE, index=False)

print(f"Dropped {len(df) - len(df_clean)} rows with NaNs.")