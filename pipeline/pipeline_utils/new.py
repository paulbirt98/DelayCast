import pandas as pd
from pipeline_utils.config import INDIVIDUAL_DIRECTIONS, INDIVIDUAL_ROUTES

stoppings_df = pd.read_csv(INDIVIDUAL_ROUTES / 'btn_bdm_route.csv')

#sort based on rid and time
stoppings_df = stoppings_df.sort_values(by=["rid", "scheduled_time"])

#set the first station for each rid as is_first_station true
stoppings_df['is_first_station'] = stoppings_df.groupby('rid')['station'].transform(
lambda x: x.eq(x.iloc[0]).astype(int)
)

#set is_terminus to true if it is the last for each rid
stoppings_df['is_terminus'] = stoppings_df.groupby('rid')['station'].transform(
lambda x: x.eq(x.iloc[-1]).astype(int)
)

stoppings_df.to_csv(INDIVIDUAL_ROUTES / 'btn_bdm_route.csv')

print('added')