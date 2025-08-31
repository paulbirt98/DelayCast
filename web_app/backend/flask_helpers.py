from datetime import timedelta
import numpy as np
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from web_app.config import WEBAPP_DB
from web_app.database.db_utils.init_db import HourlyForecast, Station
import math
from typing import Dict, List, Tuple, Iterable

def get_most_recent_forecast(session, station, now):
    """
    retrieves the most recent forecast timestamp for a given station up to the current datetime.

    Args:
    - session (session): an active SQLAlchemy session for database access.
    - station (Station): station object with a 'station_id' to identify the station in the database.
    - now (datetime): current datetime

    Returns:
    - most_recent (datetime): the most recent forecast for the given station up until the current time
    """
    if now.hour < 6:
        #get previous day
        previous_11pm = (now - timedelta(days=1)).replace(hour=23, minute=0, second=0, microsecond=0)

        most_recent = (
            session.query(HourlyForecast.timestamp_utc)
            .filter(
                HourlyForecast.station_id == station.station_id,
                HourlyForecast.timestamp_utc == previous_11pm
            )
            .limit(1)
            .scalar()
        )
    else:
        most_recent = (
            session.query(HourlyForecast.timestamp_utc).filter(
                HourlyForecast.station_id == station.station_id,
                HourlyForecast.timestamp_utc <= now
            ).order_by(HourlyForecast.timestamp_utc.desc())
            .limit(1)
            .scalar()
        )

    return most_recent

def get_top_features(feature_contributions, feature_names, k=3):
    """
    Calculates the top three features in terms of magnitude contributing to the probability prediction.

    Args:
    - feature_contributions (array): an array of feature contribution values
    - feature_names (list - str): a list of feature names

    returns:
    - output (list - dict): a list of dictionaries of feature with its corresponding contribution
    """
    idx = np.argsort(np.abs(feature_contributions))[::-1][:k]
    output = []
    for i in idx:
        output.append({
            "feature": str(feature_names[i]),
            "pp": float(feature_contributions[i] * 100.0)
        })
    return output

def get_overall_delay(probs):
    """
    Sums the probabilities (probs) of all delay classes

    Args:
    - probs (dict): the probabilties predicted

    Returns:
    - total (float): overall delay probability
    """
    total = 0.0
    for delay_class, prob in probs.items():
        if str(delay_class).strip().lower() in ("no delay", "on time", "ok", "none", "normal"):
            continue
        try:
            total += float(prob)
        except Exception:
            continue
    return total

def get_station_reference_values(bg_df, station_code, model):
    """
    gets the baselines (median) for weather featues

    args:
    - bg_df (dataframe): the testing dataframe with stoppings data for the past two years
    - station_code(str): the three letter station code
    - model : the trained model

    returns:
    - references (dict): the reference values
    """

    # station filter
    bg_df = bg_df[bg_df["station"].astype(str).str.upper() == station_code]
    sample_df = bg_df.sample(n=min(len(bg_df), 500))

    # weather medians (station-specific)
    def weather_med(col):

        return float(pd.to_numeric(sample_df.get(col), errors="coerce").median())

    ref_values = {
        "station_code": station_code,
        "temp_2m": weather_med("temperature_2m"),
        "relative_humidity": weather_med("relative_humidity_2m"),
        "rain": weather_med("rain"),
        "gusts": weather_med("wind_gusts_10m"),
        "snow_depth": weather_med("snow_depth"),
        "surface_pressure": weather_med("surface_pressure"),
    }
    return ref_values

def get_top_features(row, ref_values, feature_list, model, base_overall):
    """
    calculates the top feature drivers for a given hour using one-at-a-time (OAT) perturbation.

    args:
    - row (dict): the feature dictionary for the hour being explained
    - ref_values (dict): the reference (baseline) values for each feature at this station
    - feature_list (list): list of feature names to test (excluding station_code)
    - model: the trained calibrated model
    - base_overall (float, optional): the pre-computed overall delay probability for this hour;
      if not provided, it will be calculated internally

    returns:
    - top_features (list): a list of up to three dicts, each with:
        - feature (str): feature name
        - percentage_point (int): contribution in percentage points
    """
    try:
        base_risk = float(base_overall)
    except Exception:
        return []

    feature_impacts = []
    for feature in feature_list:
        try:
            if feature not in row:
                continue

            ref_val = ref_values.get(feature, row[feature])

            if ref_val == row[feature]:
                feature_impacts.append((feature, 0.0))
                continue

            # Create a modified copy with this feature set to the baseline value
            modified_row = dict(row)
            modified_row[feature] = ref_val

            # Get the new overall delay risk after replacing this feature
            modified_risk = get_overall_delay(model.predict_proba(modified_row))

            feature_pp = (base_risk - modified_risk) * 100.0 
            feature_impacts.append((feature, feature_pp))
        except Exception:
            feature_impacts.append((feature, 0.0))

    #sort by magnitude and get top 3
    feature_impacts.sort(key=lambda x: abs(x[1]), reverse=True)
    return [{"feature": feature, "pp": round(v)} for feature, v in feature_impacts[:3]]