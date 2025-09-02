import pandas as pd
from pipeline_utils.eng_helpers import sample_for_training
from pipeline_utils.config import GLQ_HIGH_TEMP, GLQ_HEAVY_RAIN, GLQ_HIGH_GUSTS, GLQ_HIGH_PRESSURE, GLQ_LOW_PRESSURE, GLQ_LOW_TEMP, INDIVIDUAL_ROUTES
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
    route = args.route
    try:
        training_df = pd.read_csv(INDIVIDUAL_ROUTES / route / 'binned' / f'{route}_binned_training_data.csv')
    except FileNotFoundError:
        print(f"Error: File not found")
        raise
    except pd.errors.ParserError:
        print(f"Error parsing file")
        raise
    except PermissionError:
        print(f"Permission Error with file. Ensure the file is not open elsewhere.")
        raise
    except Exception as e:
        print(f"Unexpected error reading file: {e}")

    sample_for_training(route, training_df, GLQ_LOW_TEMP, GLQ_HIGH_TEMP, GLQ_HIGH_GUSTS, GLQ_LOW_PRESSURE, GLQ_HIGH_PRESSURE, GLQ_HEAVY_RAIN)