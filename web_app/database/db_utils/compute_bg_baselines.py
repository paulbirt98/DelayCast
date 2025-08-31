import json
from pathlib import Path
from functools import lru_cache
import pandas as pd
from web_app.config import BG_PATHS, BASELINES_JSON

"""
functions and script to calculate the baseline and store in a json to save time and memory for the user when navigating to the
station info page. baselines are median values for numeric weather variables and they are percentage valeus in each delay
class for every hour fo the day, day of the week and month of the year based on the testing data set which covers ~the last two years.
"""

COLUMNS = [
    "station",
    "temperature_2m",
    "relative_humidity_2m",
    "rain",
    "wind_gusts_10m",
    "snow_depth",
    "surface_pressure",
    "hour",
    "day",
    "month",
    "delay_classification"
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
    medians = df_amalgamated.groupby("station", as_index=True).median(numeric_only=True)

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

"""
time period section
"""

#get user friendly labels
DAY_NAMES = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
MONTH_NAMES = {1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun",
                   7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"}

# time buckets
HOUR_BUCKETS  = list(range(24))
DAY_BUCKETS   = list(range(7))
MONTH_BUCKETS = list(range(1, 13))


def read_bg_dfs_time():
    frames = []
    for _, csv_path in BG_PATHS.items():
        #gilter to station
        df = pd.read_csv(csv_path, usecols=COLUMNS)
        df['station'] = df['station'].astype(str).str.upper()


        for col in ('hour', 'day', 'month'):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        # normalize class labels to clean strings
        df['delay_classification'] = df['delay_classification'].astype(str).str.strip()
        frames.append(df)
    return frames


def percentage_by_class(counts, classes):
    """
    Get the percentages in each class (rounded to 0dp)
    """
    counts = counts.reindex(classes, fill_value=0)
    total = counts.sum()
    if total == 0:
        return {cls: 0.0 for cls in classes}
    percentage = (counts / total) * 100.0
    percentage = round(percentage)
    return {cls: float(percentage[cls]) for cls in classes}


def build_time_json(station_df, classes):
    """
    builds a json to be appended to the baselines json
    """
    curves = {
        "classes_order": classes,
        "labels": {"day": DAY_NAMES, "month": MONTH_NAMES},
        "hour": {},
        "day": {},
        "month": {}
    }

    for hour in HOUR_BUCKETS:
        bucket = station_df[station_df['hour'] == hour]
        counts = bucket['delay_classification'].value_counts()
        percentage = percentage_by_class(counts, classes)
        curves["hour"][str(hour)] = percentage

    for day in DAY_BUCKETS:
        bucket = station_df[station_df['day'] == day]
        counts = bucket['delay_classification'].value_counts()
        percentage = percentage_by_class(counts, classes)
        curves["day"][str(day)] = percentage

    for month in MONTH_BUCKETS:
        bucket = station_df[station_df['month'] == month]
        counts = bucket['delay_classification'].value_counts()
        percentage = percentage_by_class(counts, classes)
        curves["month"][str(month)] = percentage

    return curves


def add_time_to_station_json():
    """
    Compute time curves for every station across all BG CSVs.
    """
    frames = read_bg_dfs_time()
    merged_df = pd.concat(frames, ignore_index=True)


    classes = sorted(merged_df['delay_classification'].dropna().unique(), key=lambda s: s.lower())

    curves_by_station = {}
    for station_code, station_df in merged_df.groupby('station'):
        curves_by_station[station_code] = build_time_json(station_df, classes)
    return curves_by_station


def merge_time_curves_into_baselines(save_file_path=BASELINES_JSON):
    """
    Load the existing baselines JSON, attach 'time_curves' for each station,
    and write it back. Returns the path.
    """
    save_path = Path(save_file_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # load existing (medians) file if present
    try:
        existing = json.loads(save_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        existing = {}
    except json.JSONDecodeError:
        # corrupted / partial file — start fresh
        existing = {}

    time_curves = add_time_to_station_json()

    # merge: add time_curves to existing stations
    for station_code, base in existing.items():
        base["time_curves"] = time_curves.get(
            station_code,
            {"classes_order": [], "labels": {"day": DAY_NAMES, "month": MONTH_NAMES},
             "hour": {}, "day": {}, "month": {}}
        )

    # include stations that exist only in time_curves
    for station_code, curves in time_curves.items():
        if station_code not in existing:
            existing[station_code] = {"station_code": station_code, "time_curves": curves}

    # pretty-print for readability
    save_path.write_text(
        json.dumps(existing, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8"
    )
    return save_path

if __name__ == "__main__":
    updated_path = merge_time_curves_into_baselines()
    print(f"Appended time-curves to baselines JSON: {updated_path}")

    
