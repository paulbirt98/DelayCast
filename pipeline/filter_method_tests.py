import pandas as pd
from pipeline_utils.config import (
    INDIVIDUAL_ROUTES, 
    UNIFIED_ROUTES_DIR,
    FS_RESULTS,
    ALL_ROUTE_FS,
    NUMERICAL_FEATURES,
    CATEGORICAL_FEATURES
)
from pipeline_utils.config import FS_RESULTS
from pipeline_utils.feature_selectors import multicoll_heatmap, run_anova_f, run_chi_squared, run_mutual_info
import argparse
import matplotlib.pyplot as plot

def argparse_cl_arguments():
    """
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", type=str, default=None)
    parser.add_argument("--filtered_features", type=str, default=None)
    return parser.parse_args()

if __name__ == '__main__':

    args = argparse_cl_arguments()
    route = args.route

    if args.filtered_features:
        filtered_out_features = [f.strip() for f in args.filtered_features.split(',')]
    else:
        filtered_out_features = []

    ##ADD ERROR HANDLING FOR ITEMS NOT IN THE LISTS
    kept_numerical_features = [f for f in NUMERICAL_FEATURES if f not in filtered_out_features]
    kept_categorical_features = [f for f in CATEGORICAL_FEATURES if f not in filtered_out_features]
    
    #if no route given, run tests on the unified dataset
    if route:
        route = route.lower()
        dataset = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_training_data.csv')
        folder = FS_RESULTS / route 
        folder.mkdir(parents=True, exist_ok=True) # make folder if it doesnt exist
        anova_filepath = folder / f'{route}_anova.csv'
        chi_filepath = folder / f'{route}_chi.csv'
        mi_filepath = folder / f'{route}_mi.csv'
    else:
        route = 'all_routes'
        kept_categorical_features = kept_categorical_features + ['route']
        dataset = pd.read_csv(UNIFIED_ROUTES_DIR / 'unified_training_data.csv')
        folder = ALL_ROUTE_FS
        anova_filepath = folder / f'{route}_anova.csv'
        chi_filepath = folder / f'{route}_chi.csv'
        mi_filepath = folder / f'{route}_mi.csv'


    anova = run_anova_f(dataset, kept_numerical_features, 'classification')
    anova.to_csv(anova_filepath, index=False)
    print('Anova')
    print(anova)
    print('\n')

    chi = run_chi_squared(dataset, kept_categorical_features)
    chi.to_csv(chi_filepath, index=False)
    print('Chi Squared')
    print(chi)
    print('\n')

    mi = run_mutual_info(dataset, kept_numerical_features, kept_categorical_features)
    mi.to_csv(mi_filepath, index=False)
    print('Mutual Info')
    print(mi)
