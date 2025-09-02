from functools import lru_cache
import json
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
STOPPINGS_DATA = PROJECT_ROOT / 'web_app' / 'database' / 'unified_routes.csv'

#static folder
STATIC_FOLDER = PROJECT_ROOT / 'web_app' / 'frontend' / 'static'

#for serving weather icons
WEATHER_ICON_DIR = STATIC_FOLDER / 'weather_icons'

TIME_GRAPH_DIR = STATIC_FOLDER / 'time_graphs'

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

BASELINES_JSON = PROJECT_ROOT / 'web_app' / 'database' / 'station_baselines.json'

@lru_cache(maxsize=1)
def load_baselines(json_path=BASELINES_JSON):
    filepath = Path(json_path)
    if not filepath.exists():
        return {}
    return json.loads(filepath.read_text())

def get_baseline_for_station(station_code):
    baselines = load_baselines().get(str(station_code).upper())
    return baselines

#design / css constants to reduce clutter on streamlit pages
TABLE_CSS = """
        <style>
        div[data-baseweb="tab-list"] {
            display: flex;
            justify-content: space-between;
            width: 100%;
        }

        div[data-baseweb="tab"] {
            flex: 1;
            text-align: center;
            font-weight: bold;
            font-size: 14px;
        }

        div[data-baseweb="tab"]:hover {
            background-color: #f0f0f0;
            border-radius: 5px;
        }
                
        div[data-baseweb="tab"][aria-selected="true"] {
            background-color: #004080;
            color: white;
            border-radius: 5px;
        }
        </style>
    """

GRAPH_CSS = """
        <style>
        .graph-wrap { margin: 10px 0 26px; }
        .graph-wrap svg { max-width: 100%; height: auto; display: block; margin: 0 auto; }
        .graph-title { text-align:center; font-weight:600; margin: 6px 0 2px; }
        .graph-caption { text-align:center; color:#6b7280; font-size: 13px; margin-top: 2px; }
        </style>
    """

LOADER_HTML = """
            <style>
            #global-loader { position: fixed; inset: 0; background: rgba(255,255,255,0.96);
            z-index: 9999; display: flex; align-items: center; justify-content: center; }
            .loader-box { text-align: center; font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
            .loader-spinner { width: 72px; height: 72px; border: 6px solid #e6e6e6; border-top-color: #111;
            border-radius: 50%; animation: spin 1s linear infinite; margin: 0 auto 16px; }
            @keyframes spin { to { transform: rotate(360deg); } }
            .loader-text { font-size: 22px; letter-spacing: 2px; font-weight: 700; }
            .subtext { font-size: 14px; color: #555; margin-top: 6px; }
            </style>
            <div id="global-loader">
            <div class="loader-box">
                <div class="loader-spinner"></div>
                <div class="loader-text">FORECASTING…</div>
                <div class="subtext">Calculating weather impacts & delay risk</div>
            </div>
            </div>
        """
