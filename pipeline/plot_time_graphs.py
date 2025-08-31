import pandas as pd
from pipeline_utils.visualisation_helpers import plot_delay_time_period
from pipeline_utils.config import INDIVIDUAL_ROUTES

filepath = INDIVIDUAL_ROUTES / 'glq_inv' / 'glq_inv_testing_data.csv'

df = pd.read_csv(filepath)

time_periods = ['hour', 'day', 'month']

for time_period in time_periods:
    plot_delay_time_period(df, 'AVM', time_period)