import pandas as pd
from pipeline_utils.config import (
    INDIVIDUAL_ROUTES,
    UNIFIED_ROUTES_DIR,
    UNIFIED_ROUTES_FILE,
    ROUTE_VALIDATION,
    ROUTE_TESTING,
    ALL_TRAINING,
    ALL_VALIDATION,
    ALL_TESTING,
    TRAINING_RATIO,
    VALIDATION_RATIO,
)
import argparse

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
    
    #if no route given, split unified dataset
    if args.route:

        route = args.route.lower()

        #assign and create file and directory paths accordingly
        route_directory = INDIVIDUAL_ROUTES / route
        route_directory.mkdir(parents=True, exist_ok=True)
        dataset = pd.read_csv(route_directory / f'{route}_route.csv', parse_dates=['scheduled_time'])

        training_file = route_directory / f'{route}_training_data.csv'
        validation_file = route_directory / f'{route}_validation_data.csv'
        testing_file = route_directory / f'{route}_testing_data.csv'

    else:
        dataset = pd.read_csv(UNIFIED_ROUTES_FILE, parse_dates=['scheduled_time'])
        training_file = UNIFIED_ROUTES_DIR / 'unified_training_data.csv'
        validation_file = UNIFIED_ROUTES_DIR / 'unified_validation_data.csv'
        testing_file = UNIFIED_ROUTES_DIR / 'unified_testing_data.csv'

    
    #sort the rows by datetime on scheduled
    dataset = dataset.sort_values(by=['scheduled_time']).reset_index(drop=True) 

    #get total number of rows
    total = len(dataset)

    training_cutoff = int(total * TRAINING_RATIO)
    validation_cutoff = training_cutoff + int(total * VALIDATION_RATIO)

    #get dataframes based on these cut offs
    training_df = dataset.iloc[:training_cutoff]
    validation_df = dataset.iloc[training_cutoff:validation_cutoff]
    testing_df = dataset.iloc[validation_cutoff:]

    print(f"Training: {training_df['scheduled_time'].min()} to {training_df['scheduled_time'].max()}")
    print(f"Validation: {validation_df['scheduled_time'].min()} to {validation_df['scheduled_time'].max()}")
    print(f"Testing: {testing_df['scheduled_time'].min()} to {testing_df['scheduled_time'].max()}")


    #save to appropriate directories
    training_df.to_csv(training_file, index=False)
    validation_df.to_csv(validation_file, index=False)
    testing_df.to_csv(testing_file, index=False)

