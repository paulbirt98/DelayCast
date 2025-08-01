import pandas as pd
from pipeline_utils.config import INDIVIDUAL_ROUTES, ALL_ROUTES_AMALG
import argparse

def argparse_cl_arguments():
    """
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", type=str, default=None)
    return parser.parse_args()

if __name__ == '__main__':

    route = argparse_cl_arguments().route.lower()
    
    #if no route given, split unified dataset
    if route:

    else:
    
