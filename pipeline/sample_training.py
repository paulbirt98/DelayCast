import pandas as pd
from pipeline_utils.eng_helpers import sample_for_training
from pipeline_utils.config import GLQ_HIGH_TEMP, GLQ_HEAVY_RAIN, GLQ_HIGH_GUSTS, GLQ_HIGH_PRESSURE, GLQ_LOW_PRESSURE, GLQ_LOW_TEMP, INDIVIDUAL_ROUTES
import argparse


def argparse_cl_arguments():
    """
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", type=str, default=None)
    return parser.parse_args()

if __name__ == '__main__':

    args = argparse_cl_arguments()
    route = args.route

    training_df = pd.read_csv(INDIVIDUAL_ROUTES / route / 'binned' / f'{route}_binned_training_data.csv')

    sample_for_training(route, training_df, GLQ_LOW_TEMP, GLQ_HIGH_TEMP, GLQ_HIGH_GUSTS, GLQ_LOW_PRESSURE, GLQ_HIGH_PRESSURE, GLQ_HEAVY_RAIN)