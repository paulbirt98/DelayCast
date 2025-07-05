import pandas as pd
import matplotlib.pyplot as plt

# Load the data
df = pd.read_csv("eda_data_with_delays_tuple.csv")

# Convert route_id from string to tuple
df['route_id'] = df['route_id'].apply(eval)

# Define the minimum frequency threshold
min_count = 100

# Count route frequencies
route_counts = df['route_id'].value_counts()
valid_routes = route_counts[route_counts >= min_count].index

# Filter the dataframe
df_filtered = df[df['route_id'].isin(valid_routes)]

# Define the stations to analyze
stations = ['EUS', 'MKC', 'CRE', 'LIV']

# Plot
fig, axes = plt.subplots(len(stations), 1, figsize=(14, 5 * len(stations)), sharex=True)

for i, station in enumerate(stations):
    delay_col = f"{station} Delay Minutes"
    avg_delay = df_filtered.groupby('route_id')[delay_col].mean().sort_values(ascending=False)
    
    avg_delay.plot(kind='bar', ax=axes[i])
    plt.setp(axes[i].xaxis.get_majorticklabels(), rotation=45, ha='right')

    axes[i].set_title(f"Average Delay at {station} by Route (Routes ≥ {min_count} services)")
    axes[i].set_ylabel("Avg Delay (mins)")
    axes[i].set_xlabel("")

plt.xlabel("Route Tuple")
plt.tight_layout()
plt.show()
