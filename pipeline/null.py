import pandas as pd
from pipeline_utils.config import INDIVIDUAL_ROUTES

# Load the file
df = pd.read_csv(INDIVIDUAL_ROUTES /"glq_inv_route.csv")

# Get columns with missing values
missing_summary = df.isnull().sum()
missing_columns = missing_summary[missing_summary > 0]

# Get rows with missing values
rows_with_missing = df[df.isnull().any(axis=1)]

# Print results
print("Columns with missing values:\n", missing_columns)
print("\nNumber of rows with missing values:", rows_with_missing.shape[0])
print("\nRows with missing values:\n", rows_with_missing)

# Check all fields in that specific row
#row = df.loc[925734]

# Show which columns are null
#print(row[row.isnull()])