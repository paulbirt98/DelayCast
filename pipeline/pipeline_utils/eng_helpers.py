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

def tvt_split(route):
    """
    """
    #assign and create file and directory paths accordingly
    route_directory = INDIVIDUAL_ROUTES / route
    route_directory.mkdir(parents=True, exist_ok=True)
    dataset = pd.read_csv(route_directory / f'{route}_route.csv', parse_dates=['scheduled_time'])

    training_file = route_directory / f'{route}_training_data.csv'
    validation_file = route_directory / f'{route}_validation_data.csv'
    testing_file = route_directory / f'{route}_testing_data.csv'

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

def temperature_binning_glq(temperature):
        """
        
        """
        if pd.isna(temperature):
            return "Issue Binning"
        elif temperature <= -3:
            return "Very Cold"
        elif -3 < temperature <= 2:
            return "Cold"
        elif 2 < temperature <= 10:
            return "Cool"
        elif 10 < temperature <= 20:
            return "Mild"
        elif 20 < temperature <= 23:
            return "Warm"
        else:
             return "Hot"
        
def snow_depth_binning_glq(snow_depth):
        """
        
        """
        if pd.isna(snow_depth):
            return "Issue Binning"
        elif snow_depth <= 0:
            return "No Lying Snow"
        elif 0 < snow_depth <= 0.03:
            return "Dusting"
        elif 0.03 < snow_depth <= 0.1:
            return "Substantial"
        elif 10 < snow_depth:
            return "Deep"

def rain_binning_glq(rain):
        """
        
        """
        if pd.isna(rain):
            return "Issue Binning"
        elif rain <= 0:
            return "No Rain"
        elif 0 < rain <= 0.5:
            return "Light"
        elif 0.5 < rain <= 1:
            return "Heavy"
        elif  1 < rain:
            return "Very Heavy"
        
def gust_binning_glq(gusts):
        """
        
        """
        if pd.isna(gusts):
            return "Issue Binning"
        elif gusts <= 20:
            return "Calm"
        elif 20 < gusts <= 40:
            return "Breezy"
        elif 40 < gusts <= 60:
            return "Windy"
        elif 60 < gusts <= 75:
            return "Gale Force"
        elif  75 < gusts:
            return "Severe Gale"