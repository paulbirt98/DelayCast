import pandas as pd
from pipeline_utils.config import (
    INDIVIDUAL_ROUTES, 
    ALL_ROUTES_AMALG,
    FILTER_RESULTS
)
from pipeline_utils.config import FILTER_RESULTS
from pipeline_utils.feature_selectors import run_anova_f, run_mutual_info
import argparse
import numpy as np

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
        dataset = pd.read_csv(INDIVIDUAL_ROUTES / f'{route}_route.csv')
    else:
        dataset = pd.read_csv(ALL_ROUTES_AMALG)

    #shuffled_target = np.random.permutation(dataset['delay_classification'])
    
    #run_anova_f(dataset.assign(delay_classification=shuffled_target), p_threshold=0.05)

    run_mutual_info(dataset)

