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
        parses command-line arguments so they are accessible within the add_new_route script

        Args:
        - read from command line

        Command-line Arguments:
        - --route(str): the route identifier in format 'glq_inv.

        Returns:
        - parser.parse_args(): the parsed arguments accessible as attributes.
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
        dataset = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_training_data.csv')
        folder = FS_RESULTS / route 
        folder.mkdir(parents=True, exist_ok=True) # make folder if it doesnt exist
        heat_map_filepath = folder / f'{route}_heatmap.png'
    else:
        route = 'all_routes'
        dataset = pd.read_csv(UNIFIED_ROUTES_DIR / 'unified_training_data.csv')
        folder = ALL_ROUTE_FS
        heat_map_filepath = FS_RESULTS / route / f'{route}_heatmap.png'

    #get and save multicollinearity heat map
    heat_map = multicoll_heatmap(dataset)
    #save to folder
    heat_map.savefig(heat_map_filepath, dpi=300, bbox_inches='tight')
    plot.show()