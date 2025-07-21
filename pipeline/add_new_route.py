from pathlib import Path
import pandas as pd
from datetime import timedelta
from pipeline_utils.config import FROM_DATE, TO_DATE, RAW_DATA
import argparse
from pipeline_utils.api_helpers import fetch_rids, fetch_train_times

#Parse in args for route beginning and end, toc, and testing boolean from the command line
def parse_cl_arguments():
    """
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--from_location", type=str, required=True)
    parser.add_argument("--to_location", type=str, required=True)
    parser.add_argument("--toc", type=str, required=True)
    parser.add_argument("--testing", action="store_true")
    return parser.parse_args()

args = parse_cl_arguments()
from_location = args.from_location.upper()
to_location = args.to_location.upper()
toc = args.toc.upper()
testing = args.testing

#call api helper functions and assign
rid_df = fetch_rids(from_location, to_location, toc, FROM_DATE, TO_DATE, testing=testing)
train_data_df = fetch_train_times(rid_df)

#build filepath to save raw data to
save_file_path = RAW_DATA / f'{from_location.lower()}_{to_location.lower()}_raw.csv'

try:
    train_data_df.to_csv(save_file_path, index=False)
    print("Train details saved successfully")
except PermissionError:
    print(f"Error saving. File {save_file_path} appears to be open. Please close the file and try again")



