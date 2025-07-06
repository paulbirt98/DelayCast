import pandas as pd

df = pd.read_csv("eus_liv_2015-25.csv")

core_stations = ['EUS', 'WFJ', 'MKC', 'RUG', 'NUN', 'TAM', 'LTV', 'STA', 'CRE', 'RUN', 'LIV']

for i, station in enumerate(core_stations):

    if i < (len(core_stations) -1):
        scheduled = f"{station} Scheduled Departure Time"
        actual = f"{station} Actual Departure Time"
        