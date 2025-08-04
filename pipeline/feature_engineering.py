import pandas as pd
from pipeline_utils.config import UNIFIED_ROUTES_FILE, UNIFIED_ROUTES_DIR

data = pd.read_csv(UNIFIED_ROUTES_DIR / 'unified_training_data.csv')

data['route'] = data['direction'].apply(lambda x: '-'.join(sorted(x.split('-'))))

route_counts = data.groupby('route').size().reset_index(name='count')

print(route_counts)