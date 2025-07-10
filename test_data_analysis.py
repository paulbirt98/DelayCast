import pandas as pd
import matplotlib.pyplot as plt

train_df = pd.read_csv('eus_liv_joined.csv')

# Define custom bins per variable to capture extremes
binning_strategies = {
    'temperature_2m': [-5, 0, 10, 20, 25, 30],  # e.g. extreme cold, cold, mild, warm, hot, very hot
    'rain': [0, 1, 5, 10, 20, 100],                 # dry, light, moderate, heavy, extreme
    'snow_depth': [0, 0.01, 0.03, 0.05, 0.1],               # no snow, dusting, snow, deep snow
    'wind_speed_10m': [0, 10, 20, 30, 40, 100],     # calm, breeze, windy, very windy, storm
    'wind_gusts_10m': [0, 20, 30, 40, 50, 150]      # calm, gusty, strong gusts, severe gusts, extreme gusts
}

for feature, bins in binning_strategies.items():
    train_df[f'{feature}_bin'] = pd.cut(train_df[feature], bins=bins, include_lowest=True)

    # Group by bin and Delay Classification, count occurrences
    grouped = train_df.groupby([f'{feature}_bin', 'Delay Classification']).size().unstack(fill_value=0)

    # Calculate percentage per bin
    percentage = grouped.div(grouped.sum(axis=1), axis=0) * 100

    # Plotting
    percentage.plot(kind='bar', stacked=True, figsize=(10, 6))
    plt.title(f"Percentage of Delay Classes by {feature} bin (extremes separated)")
    plt.ylabel("Percentage")
    plt.xlabel(f"{feature} bins")
    plt.legend(title="Delay Classification")
    plt.tight_layout()
    plt.show()



