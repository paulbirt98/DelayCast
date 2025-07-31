import os
from dotenv import load_dotenv
import base64
from pathlib import Path
from datetime import datetime, timedelta

def build_headers_object():
    """
    Function to build a headers object to be used in HSP API calls

    Returns:
    A dict called headers consisting of Content-Type: application/json and Authorization: base64 encoded authorisation header
    using HSP API credentials (email and password)
    """
    load_dotenv()    #Access email and HSP password env variables
    hsp_email = os.getenv('HSP_EMAIL')
    hsp_password = os.getenv('HSP_PASSWORD')

    #encode authorisation header and set headers constant for HSP API calling methods
    auth_string = f"{hsp_email}:{hsp_password}"
    auth_header = base64.b64encode(auth_string.encode()).decode()

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_header}"
    }

    return headers

#Headers dictionary to be used in HSP API calls
HEADERS = build_headers_object()

#HSP API URLs
METRICS_URL = "https://hsp-prod.rockshore.net/api/v1/serviceMetrics"
DETAILS_URL = "https://hsp-prod.rockshore.net/api/v1/serviceDetails"

#Project root path
PROJECT_ROOT  = Path(__file__).resolve().parents[2]

#Data processing file paths
DATA = PROJECT_ROOT / 'data'
RAW_DATA = DATA / 'raw_api_responses'
INTERIM_DATA = DATA / 'semi_processed'
PROCESSED_DATA = DATA / 'processed'
INDIVIDUAL_DIRECTIONS = PROCESSED_DATA / 'individual' / 'directions'
INDIVIDUAL_ROUTES = PROCESSED_DATA / 'individual' / 'routes'
ALL_ROUTES_AMALG = PROCESSED_DATA / 'amalgamated_routes'
METADATA = DATA / 'metadata'
UK_STATIONS_FILE = METADATA / 'uk_stations.csv'

#Pipeline file path
PIPELINE = PROJECT_ROOT / 'pipeline'

#Feature selection filepaths
FILTER_RESULTS = PIPELINE / 'results' / 'filter_results'

#Date Ranges for HSP API call
FROM_DATE = datetime(2015, 6, 1)
TO_DATE = datetime(2025, 5, 31) 

#Daily time window for HSP API call (24 hour clock)
FROM_TIME = 6
TO_TIME = 22

#max workers for thread pool executor
MAX_WORKERS = 3

#Thresholds for removing infrequent and not recent station codes (percentage)
FREQ_VALUE = 0.05
CUT_OFF_DATE = datetime(2025, 5, 31) - timedelta(days=182) #approx 6 months

#For use in delay classification
RECORDING_ERROR_MIN = -720
RECORDING_ERROR_MAX = 720
NO_DELAY_UPPER_BOUNDARY = 5
MILD_DELAY_UPPER_BOUNDARY = 15
MODERATE_DELAY_UPPER_BOUNDARY = 30

#For use in feature selection
NUMERICAL_FEATURES = [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "rain",
            "snowfall",
            "snow_depth",
            "surface_pressure",
            "cloud_cover",
            "soil_temperature_0_to_7cm",
            "soil_moisture_0_to_7cm",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
    ]
CATEGORICAL_FEATURES = [
    'toc',
    'station',
    'hour',
    'day',
    'month',
    'direction',
    'is_day'
]