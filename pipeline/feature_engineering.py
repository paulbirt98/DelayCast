import pandas as pd
from pipeline_utils.config import UNIFIED_ROUTES_FILE, UNIFIED_ROUTES_DIR, INDIVIDUAL_ROUTES
from pipeline_utils.preproccesing_helpers import calculate_delay_classification
from pipeline_utils.eng_helpers import tvt_split, threeclass_delay_classification, binary_delay_classification

#assign route
route = 'eus_liv'
route_filepath = INDIVIDUAL_ROUTES / route / f'{route}_route.csv'

#read file and recalculate delay classes
data = pd.read_csv(route_filepath)

print(data['delay_classification'].value_counts())

data['delay_classification'] = data['delay_minutes'].apply(calculate_delay_classification)

#save over file
data.to_csv(route_filepath, index=False)

print(data['delay_classification'].value_counts())

#split
#tvt_split(route)

