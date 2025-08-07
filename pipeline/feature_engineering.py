import pandas as pd
from pipeline_utils.config import UNIFIED_ROUTES_FILE, UNIFIED_ROUTES_DIR, INDIVIDUAL_ROUTES

route = 'glq_inv'

train = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_training_data.csv')
val = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_validation_data.csv')
test = pd.read_csv(INDIVIDUAL_ROUTES / route / f'{route}_testing_data.csv')

delay_counts_train = train['delay_classification'].value_counts().reset_index()
delay_counts_train.columns = ['delay_classification', 'count']
print(delay_counts_train)

delay_counts_val = val['delay_classification'].value_counts().reset_index()
delay_counts_val.columns = ['delay_classification', 'count']
print(delay_counts_val)

delay_counts_test = test['delay_classification'].value_counts().reset_index()
delay_counts_test.columns = ['delay_classification', 'count']
print(delay_counts_test)