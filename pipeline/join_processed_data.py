import pandas as pd
import argparse
from pipeline_utils.config import (
    INDIVIDUAL_DIRECTIONS,
    INDIVIDUAL_ROUTES,
)
from pipeline_utils.preproccesing_helpers import concat_stoppings_dfs

def parse_cl_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--startpoint", type=str, required=True)
    parser.add_argument("--endpoint", type=str, required=True)
    return parser.parse_args()

if __name__ == '__main__':

    #assign start and end points
    args = parse_cl_arguments()
    startpoint = args.startpoint.lower()
    endpoint = args.endpoint.lower()

    #check to make sure both files exist
    outwards_filepath = INDIVIDUAL_DIRECTIONS / f'{startpoint}_{endpoint}_final.csv'
    return_filepath = INDIVIDUAL_DIRECTIONS / f'{endpoint}_{startpoint}_final.csv'
    
    if outwards_filepath.is_file() and return_filepath.is_file():

        route = f'{startpoint}_{endpoint}'
        print(f'Merging both directions on {route}')

        try:
            outwards_df = pd.read_csv(outwards_filepath)
            return_df = pd.read_csv(return_filepath)
        except PermissionError:
            print(f"Permission error in accessing files, please ensure neither are open elsewhere")
            exit(1)
        except Exception as e:
            print(f"Unexpected error loading file: {e}")
            exit(1)

        #merge and save
        route_df = concat_stoppings_dfs(outwards_df, return_df)
        route_filepath = INDIVIDUAL_ROUTES / route / f'{startpoint}_{endpoint}_route.csv'
        route_df.to_csv(route_filepath, index=False)

        print('Success. Saved to individual routes')
    
    else:
        print('Error: Please check both files exist.')