import pandas as pd
import requests_cache
import time
import openmeteo_requests
from retry_requests import retry
from pipeline_utils.config import INDIVIDUAL_DIRECTIONS, INDIVIDUAL_ROUTES
from pipeline_utils.preproccesing_helpers import load_json_to_dict

df = pd.read_csv(INDIVIDUAL_ROUTES / "glq_inv_route.csv")

#sort based on rid and time
df = df.sort_values(by=["rid", "scheduled_time"])

#set the first station for each rid as is_first_station true
df['is_first_station'] = df.groupby('rid')['station'].transform(
lambda x: x.eq(x.iloc[0]).astype(int)
)

#set is_terminus to true if it is the last for each rid
df['is_terminus'] = df.groupby('rid')['station'].transform(
lambda x: x.eq(x.iloc[-1]).astype(int)
)

# Find the index of the 'direction' column
direction_idx = df.columns.get_loc('direction')

# Move the new columns just after 'direction'
df.insert(direction_idx + 1, 'is_first_station', df.pop('is_first_station'))
df.insert(direction_idx + 2, 'is_terminus', df.pop('is_terminus'))

df.to_csv(INDIVIDUAL_ROUTES / 'glq_inv_route.csv', index=False)
