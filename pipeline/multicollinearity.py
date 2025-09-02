import pandas as pd
from pipeline_utils.config import (
    INDIVIDUAL_ROUTES, 
    UNIFIED_ROUTES_DIR,
    FS_RESULTS,
    ALL_ROUTE_FS
)
from pipeline_utils.config import FS_RESULTS
from pipeline_utils.feature_selectors import multicoll_heatmap
import argparse
import matplotlib.pyplot as plot

def argparse_cl_arguments():
    """
        parses command-line arguments

        args:
        - --route(str): the route identifier in format 'glq_inv.
       
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", type=str, default=None)
    return parser.parse_args()

if __name__ == '__main__':

    args = argparse_cl_arguments()
    route = args.route
    
    #if no route given, run tests on the unified dataset
    if route:
        route = route.lower()
        file = INDIVIDUAL_ROUTES / route / f'{route}_training_data.csv'

        try:
            dataset = pd.read_csv(file)
        except FileNotFoundError:
            print(f"Error: File {file} not found")
            raise
        except pd.errors.ParserError:
            print(f"Error parsing file {file}")
            raise
        except PermissionError:
            print(f"Permission Error with file {file}. Ensure the file is not open elsewhere.")
            raise
        except Exception as e:
            print(f"Unexpected error reading file {file}: {e}")
        
        folder = FS_RESULTS / route 
        folder.mkdir(parents=True, exist_ok=True) # make folder if it doesnt exist
        heat_map_filepath = folder / f'{route}_heatmap.png'
    else:
        route = 'all_routes'
        file = UNIFIED_ROUTES_DIR / 'unified_training_data.csv'
        try:
            dataset = pd.read_csv(file)
        except FileNotFoundError:
            print(f"Error: File {file} not found")
            raise
        except pd.errors.ParserError:
            print(f"Error parsing file {file}")
            raise
        except PermissionError:
            print(f"Permission Error with file {file}. Ensure the file is not open elsewhere.")
            raise
        except Exception as e:
            print(f"Unexpected error reading file {file}: {e}")
        folder = ALL_ROUTE_FS
        heat_map_filepath = FS_RESULTS / route / f'{route}_heatmap.png'

    #get and save multicollinearity heat map
    heat_map = multicoll_heatmap(dataset)
    #save to folder
    heat_map.savefig(heat_map_filepath, dpi=300, bbox_inches='tight')
    plot.show()