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