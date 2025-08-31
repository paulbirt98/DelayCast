from functools import lru_cache
from pathlib import Path
import os
from dotenv import load_dotenv
import pandas as pd

from web_app.backend.model_inference import DelayRiskModel

load_dotenv()

#deployment url or localhost port no. as default during development
FLASK_API_URL = os.getenv("FLASK_API_URL", "http://127.0.0.1:5000")

#set project root file path
PROJECT_ROOT = Path(__file__).resolve().parents[1]

#get data sources (from env if theyre set, otherwise from via Path)
METADATA_LOCAL_PATH = PROJECT_ROOT / 'data' / 'metadata'
METADATA_DIR = Path(os.getenv("METADATA", METADATA_LOCAL_PATH)).resolve()

NF_CORE = PROJECT_ROOT / 'web_app' / 'database' / 'nf_core.csv'
STOPPINGS_DATA = PROJECT_ROOT / 'data' / 'processed' / 'unified_routes' / 'unified_routes.csv'

#for serving weather icons
WEATHER_ICON_DIR = PROJECT_ROOT / 'web_app' / 'frontend' / 'static' / 'weather_icons'

#path for database initialisations
WEBAPP_DB = PROJECT_ROOT / 'web_app' / 'database' / 'web_app.db'

#length of forecast(days)
FORECAST_LENGTH = 5 

#weather icon boundaries
WINDY = 50
HOT = 25
COLD = 0

WANTED_ELRS = {
    'gwr': {
        "lines": ['MLN1','MLN2','MLN3','MLN4','BHL','SWY','CCL','WEY'],
        "colour": [0, 255, 0]
    },
    'sr': {
        "lines": ['HGL2', 'HGL1','SCM4','SCM3','EGM1'],
        "colour": [0, 0, 255]
    },
    'vt': {
        "lines": ['LEC1', 'LEC2', 'LEC4', 'CGJ1', 'WJL1', 'WJL2', 'WJL3', 'WJL4'],
        "colour": [255, 255, 0]
    },
    'tl': {
        "lines": ['VTB3', 'VTB2', 'LBW', 'BMJ', 'HHH', 'FTL', 'SPC1', 'MCL'],
        "colour": [255, 0, 0]
    }
}

#station - route lookup dict
STATIONS_BY_ROUTE = {
    "glq_inv": ["AVM","BLA","CAG","DLW","DBL","DKD","GLQ","GLE","INV","KIN","LBT","NWR","PTH","PIT","STG"],
    "btn_bdm": ["BAB","BDM","BTN","BUG","CTK","ECR","ZFD","FLT","GTW","HLN","HPD","HSK","HHE","LEA","BFR",
                "LBG","STP","LUT","LTN","PRP","SAC","TBD","WHP","WVF"],
    "eus_liv": ["CRE","LIV","EUS","MKC","NUN","RUG","RUN","STA","LTV","TAM"],
    "pad_pnz": ["BOD","BRI","BTH","CBN","CLC","CPM","DID","EXD","HYL","IVY","LOS","LSK","NBY","NTA","PAD",
                "PAR","PEW","PLY","PNZ","RDG","RED","SAU","SER","SGM","STS","SWI","TAU","TOT","TRU","TVP","WSB"]
}

MODELS_DIR = PROJECT_ROOT / 'web_app' / 'models'

#function to return correct model
def get_route_code(station_code):
    """
    returns the relevant route code given the passed station code

    args:
    -station_code (str): three letter station code

    returns:
    - the relevant route code
    """
    if station_code.upper() in STATIONS_BY_ROUTE.get('glq_inv'):
        return 'glq_inv'
    elif station_code.upper() in STATIONS_BY_ROUTE.get('btn_bdm'):
        return 'btn_bdm'
    elif station_code.upper() in STATIONS_BY_ROUTE.get('eus_liv'):
        return 'eus_liv'
    elif station_code.upper() in STATIONS_BY_ROUTE.get('pad_pnz'):
        return 'pad_pnz'
    else:
        raise ValueError(f'No model found for {station_code}')

#get relevant model per station - ensure only one model cached at a time
@lru_cache(maxsize=1)
def get_model_for_route(route_code):
    return DelayRiskModel(MODELS_DIR / route_code)

def get_model_for_station(station_code):
    route = get_route_code(station_code)
    return get_model_for_route(route)
    

##get relevant background df - only one cached
glq_bg_data_route = PROJECT_ROOT / 'data' / 'processed' / 'individual' / 'routes' / 'glq_inv' / 'glq_inv_testing_data.csv'
btn_bg_data_route = PROJECT_ROOT / 'data' / 'processed' / 'individual' / 'routes' / 'btn_bdm' / 'btn_bdm_testing_data.csv'
eus_bg_data_route = PROJECT_ROOT / 'data' / 'processed' / 'individual' / 'routes' / 'eus_liv' / 'eus_liv_testing_data.csv'
pad_bg_data_route = PROJECT_ROOT / 'data' / 'processed' / 'individual' / 'routes' / 'pad_pnz' / 'pad_pnz_testing_data.csv'

BG_PATHS = {
    "glq_inv": glq_bg_data_route,
    "btn_bdm": btn_bg_data_route,
    "eus_liv": eus_bg_data_route,
    "pad_pnz": pad_bg_data_route,
}

@lru_cache(maxsize=1)
def get_bg_df_for_route(route_code):
    path = BG_PATHS[route_code]
    return pd.read_csv(path)

def get_bg_df_for_station(station_code):
    route = get_route_code(station_code)
    return get_bg_df_for_route(route)