import pandas as pd
from pipeline_utils.config import (
    INDIVIDUAL_ROUTES, 
    ALL_ROUTES_AMALG,
    FS_RESULTS,
    ALL_ROUTE_FS
)
from pipeline_utils.config import FS_RESULTS
from pipeline_utils.feature_selectors import multicoll_heatmap, run_anova_f, run_chi_squared, run_mutual_info
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
        folder = FS_RESULTS / route 
        folder.mkdir(parents=True, exist_ok=True) # make folder if it doesnt exist
        heat_map_filepath = folder / f'{route}_route_heatmap.png'
        anova_filepath = folder / f'{route}_anova.csv'
        chi_filepath = folder / f'{route}_chi.csv'
        mi_filepath = folder / f'{route}_mi.csv'
    else:
        dataset = pd.read_csv(ALL_ROUTES_AMALG)
        folder = ALL_ROUTE_FS

    #get and save multicollinearity heat map
    heat_map = multicoll_heatmap(dataset)
    #save to folder
    heat_map.savefig(heat_map_filepath, dpi=300, bbox_inches='tight')
    
    anova = run_anova_f(dataset, 'classification')
    anova.to_csv(anova_filepath, index=False)
    print('Anova')
    print(anova)
    print('\n')

    chi = run_chi_squared(dataset)
    chi.to_csv(chi_filepath, index=False)
    print('Chi Squared')
    print(chi)
    print('\n')

    mi = run_mutual_info(dataset)
    mi.to_csv(mi_filepath, index=False)
    print('Mutual Info')
    print(mi)
