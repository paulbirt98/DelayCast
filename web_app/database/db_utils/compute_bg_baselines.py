import json
from pathlib import Path
from functools import lru_cache
import pandas as pd
from web_app.config import BG_PATHS, BASELINES_JSON

"""
functions and script to calculate the baselines and store in a json to save time and memory for the user when navigating to the
station info page. baselines are median values for numeric weather variables based on the testing data set which covers ~the last two years.
"""

COLUMNS = [
    "station",
    "temperature_2m",
    "relative_humidity_2m",
    "rain",
    "wind_gusts_10m",
    "snow_depth",
    "surface_pressure",
]

def read_bg_dfs():
    """
    Reads all backgroudn (testing) dataframes and returns a list containing each
    """
    dataframes = []
    for _, path in BG_PATHS.items():
        df = pd.read_csv(path, usecols=COLUMNS)
        df["station"] = df["station"].astype(str).str.upper()
        dataframes.append(df)
    return dataframes

def compute_station_medians():
    """
    Gets the station baseline medians for all weather variables

    Returns:
    - baselines (JSON): format {STATION: {temp_2m, relative_humidity, rain, gusts, snow_depth, surface_pressure}}.
    """
    df_amalgamated = pd.concat(read_bg_dfs(), ignore_index=True)
    medians = df_amalgamated.groupby("station", as_index=True).median(numeric_only=True).fillna(0)

    baselines = {}
    for station, row in medians.iterrows():
        baselines[station] = {
            "station_code": station,
            "temp_2m": float(row["temperature_2m"]),
            "relative_humidity": float(row["relative_humidity_2m"]),
            "rain": float(row["rain"]),
            "gusts": float(row["wind_gusts_10m"]),
            "snow_depth": float(row["snow_depth"]),
            "surface_pressure": float(row["surface_pressure"]),
        }
    return baselines

def build_and_save(save_file_path=BASELINES_JSON):
    """
    writes the baselines to a json and returns the filepath
    """
    save_path = Path(save_file_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)

    baselines = compute_station_medians()
    save_path.write_text(json.dumps(baselines, indent=2), encoding="utf-8")

    return save_path

if __name__ == "__main__":
    save_file_path = build_and_save()
    print(f"Wrote weather baselines to: {save_file_path}")