import pandas as pd
from pipeline_utils.config import INDIVIDUAL_ROUTES, UNIFIED_ROUTES_DIR, SAMPLE_SIZE, MAX_RARE_WEATHER

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

                    #identify rare but important weather variables
                    rare_weather = route_df[
                        (route_df['snow_depth'] > 0) |
                        (route_df['temperature_2m'] < 0) |
                        (route_df['temperature_2m'] > 25) |
                        (route_df['wind_gusts_10m'] > 60)
                    ]

                    print(f'Initial rare weather rows found: {len(rare_weather)}')

                    #remove rare weather to sample equal number from each route
                    common_weather = route_df.drop(rare_weather.index)

                    rare_weather = rare_weather.sample(n=min(len(rare_weather), MAX_RARE_WEATHER), random_state=42)


                    #sample equal numebr of rows from each route
                    if len(common_weather) >= SAMPLE_SIZE:
                        sample_data = common_weather.sample(n=SAMPLE_SIZE, random_state=42)
                    else:
                        sample_data = common_weather
                        print(f'Less than {SAMPLE_SIZE} rows for {route}, number of rows sampled {len(common_weather)}')

                    #combine with rare weather
                    final_sample = pd.concat([sample_data, rare_weather], ignore_index=True)

                    #add a route feature
                    # 👉 Add route identifier as a feature
                    final_sample['route'] = route

                    print(f'{len(rare_weather)} rare event rows added back for {route}')
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