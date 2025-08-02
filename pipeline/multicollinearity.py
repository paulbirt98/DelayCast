import pandas as pd
from pipeline_utils.config import (
    INDIVIDUAL_ROUTES, 
    ALL_ROUTES_AMALG,
    FS_RESULTS,
    ALL_ROUTE_FS
)
from pipeline_utils.config import FS_RESULTS
from pipeline_utils.feature_selectors import multicoll_heatmap
import argparse
import matplotlib.pyplot as plot

def argparse_cl_arguments():
    """
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", type=str, default=None)
    return parser.parse_args()

if __name__ == '__main__':

    route = argparse_cl_arguments().route.lower()
    
    #if no route given, run tests on the unified dataset
    if route:
        dataset = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_route.csv')
        folder = FS_RESULTS / route 
        folder.mkdir(parents=True, exist_ok=True) # make folder if it doesnt exist
        heat_map_filepath = folder / f'{route}_route_heatmap.png'
    else:
        dataset = pd.read_csv(ALL_ROUTES_AMALG)
        folder = ALL_ROUTE_FS

    #get and save multicollinearity heat map
    heat_map = multicoll_heatmap(dataset)
    #save to folder
    heat_map.savefig(heat_map_filepath, dpi=300, bbox_inches='tight')
    plot.show()