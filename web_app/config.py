from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

#deployment url or localhost port no. as default during development
FLASK_API_URL = os.getenv("FLASK_API_URL", "http://127.0.0.1:5000")

#set project root file path
PROJECT_ROOT = Path(__file__).resolve().parents[1]

#get data sources (from env if theyre set, otherwise from via Path)
METADATA_LOCAL_PATH = PROJECT_ROOT / 'data' / 'metadata'
METADATA_DIR = Path(os.getenv("METADATA", METADATA_LOCAL_PATH)).resolve()

WANTED_ELRS = {
    'gwr': {
        "lines": ['MLN1','MLN2','MLN3','MLN4','BHL','SWY','CCL','WEY'],
        "colour": "green"
    },
    'sr': {
        "lines": ['HGL2', 'HGL1','SCM4','SCM3','EGM1'],
        "colour": "blue"
    },
    'vt': {
        "lines": ['LEC1', 'LEC2', 'LEC4', 'CGJ1', 'WJL1', 'WJL2', 'WJL3', 'WJL4'],
        "colour": "yellow"
    },
    'tl': {
        "lines": ['VTB3', 'VTB2', 'LBW', 'BMJ', 'HHH', 'FTL', 'SPC1', 'MCL'],
        "colour": "red"
    }
}