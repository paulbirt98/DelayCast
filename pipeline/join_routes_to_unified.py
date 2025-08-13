import pandas as pd
from pipeline_utils.config import INDIVIDUAL_ROUTES, UNIFIED_ROUTES_DIR, SAMPLE_SIZE, MAX_PROTECTED_SAMPLE

if __name__ == '__main__':

    all_routes = []

    #loop through all the 'route' directories and add to list
    for directory in INDIVIDUAL_ROUTES.iterdir():
        for file in directory.iterdir():
            if file.name.endswith('_route.csv'):
                try:
                    route_df = pd.read_csv(file)
                    route = file.stem.replace('_route', '')

                    print(f'\nNumber of rows in {route}: {len(route_df)}')

                    #identofy minority classes to preserve
                    moderate_rows = route_df[route_df['delay_classification'] == 'Moderate Delay']
                    severe_rows = route_df[route_df['delay_classification'] == 'Severe Delay']

                    print(f"Keeping {len(moderate_rows)} Moderate, {len(severe_rows)} Severe")

                    minority_rows = pd.concat([moderate_rows, severe_rows], ignore_index=False)
                    minority_rows = minority_rows.sample(n=min(len(minority_rows), MAX_PROTECTED_SAMPLE), random_state=42)

                    #remove minority classes from main df
                    records_to_protect = minority_rows.index
                    unprotected_records = route_df.drop(minority_rows.index)

                    #identify rare but important weather variables
                    rare_weather = unprotected_records[
                        (unprotected_records['snow_depth'] > 0) |
                        (unprotected_records['temperature_2m'] < 0) |
                        (unprotected_records['temperature_2m'] > 25) |
                        (unprotected_records['wind_gusts_10m'] > 60) |
                        (unprotected_records['surface_pressure'] < 980)
                    ]

                    print(f'Additional rare weather rows found: {len(rare_weather)}')

                    #remove rare weather to sample equal number from each route
                    rare_weather = rare_weather.sample(n=min(len(rare_weather), MAX_PROTECTED_SAMPLE), random_state=42)
                    rare_weather_records = rare_weather.index
                    unprotected_records = unprotected_records.drop(rare_weather_records)

                    #sample equal numebr of rows from each route
                    if len(unprotected_records) >= SAMPLE_SIZE:
                        sample_data = unprotected_records.sample(n=SAMPLE_SIZE, random_state=42)
                    else:
                        sample_data = unprotected_records
                        print(f'Less than {SAMPLE_SIZE} rows for {route}, number of rows sampled {len(unprotected_records)}')

                    #combine with rare weather
                    final_sample = pd.concat([sample_data, rare_weather, minority_rows], ignore_index=True)

                    #add a route feature
                    final_sample['route'] = route

                    print(f'{len(rare_weather)} rare event rows added back for {route}')
                    print(f'{len(minority_rows)} moderate and severe delay rows added back for {route}')
                    print(f'Final sample size for {route}: {len(final_sample)} rows')

                    all_routes.append(final_sample)
                    print(f'{route} added to the list')
                except Exception as e:
                    print(f'Error reading file {file}: {e}')
    
#concatenate all dfs in the list
print('Concatenating files into unified dataframe')
unified_data_df = pd.concat(all_routes, ignore_index=True)
print('Dataframes successfully concatenated')

# Drop any rows with at least one NaN
unified_data_df_clean = unified_data_df.dropna()

print(f"Dropped {len(unified_data_df) - len(unified_data_df_clean)} rows with NaNs.")

save_filepath = UNIFIED_ROUTES_DIR / 'unified_routes.csv'

unified_data_df_clean.to_csv(save_filepath, index=False)
print(f'Unified data saved to {save_filepath}')