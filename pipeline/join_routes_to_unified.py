import pandas as pd
from pipeline_utils.config import INDIVIDUAL_ROUTES, UNIFIED_ROUTES

if __name__ == '__main__':

    all_routes = []

    #loop through all the 'route' directories and add to list
    for directory in INDIVIDUAL_ROUTES.iterdir():
        for file in directory.iterdir():
            if file.name.endswith('_route.csv'):
                try:
                    route_df = pd.read_csv(file)
                    all_routes.append(route_df)
                    print(f'{file} added to the list')
                except Exception as e:
                    print(f'Error reading file {file}: {e}')
    
#concatenate all dfs in the list
print('Concatenating files into unified dataframe')
unified_data_df = pd.concat(all_routes, ignore_index=True)
print('Dataframes successfully concatenated')

save_filepath = UNIFIED_ROUTES / 'unified_routes.csv'

unified_data_df.to_csv(save_filepath, index=False)
print(f'Unified data saved to {save_filepath}')