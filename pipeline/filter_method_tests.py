import pandas as pd
from pipeline_utils.config import (
    INDIVIDUAL_ROUTES, 
    ALL_ROUTES_AMALG,
    FILTER_RESULTS
)
from pipeline_utils.config import FILTER_RESULTS
import argparse

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
        route_file = pd.read_csv(ALL_ROUTES_AMALG)
    else:
        route_file = pd.read_csv(INDIVIDUAL_ROUTES / f'{route}.csv')
    
    
