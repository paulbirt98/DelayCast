import pandas as pd
import requests_cache
import time
import openmeteo_requests
from retry_requests import retry
from pipeline_utils.config import INDIVIDUAL_DIRECTIONS
from pipeline_utils.preproccesing_helpers import load_json_to_dict

# ---------- core fetcher (is_day only) ----------

def fetch_is_day(lat, lon, start_date, end_date, timezone="Europe/London"):
    """
    Fetch hourly is_day for [start_date, end_date] (inclusive) for a single lat/lon.
    Returns a dataframe with a local (Europe/London) naive timestamp column named 'time'
    and an int8 'is_day' column.
    """
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["is_day"],
        "timezone": timezone
    }

    responses = client.weather_api(url, params=params)
    hourly = responses[0].Hourly()

    is_day = hourly.Variables(0).ValuesAsNumpy()

    # Open-Meteo gives unix timestamps (seconds). Build the hourly index then convert to Europe/London.
    rng_utc = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
    # Convert to Europe/London (to match your df) and drop tz info for a clean merge
    rng_local = rng_utc.tz_convert(timezone)

    out = pd.DataFrame({"time": rng_local, "is_day": is_day.astype("int8")})
    return out

# ---------- wrapper that merges onto your df by nearest_hour ----------

def add_is_day_using_nearest_hour(df, stations, timezone="Europe/London"):
    """
    For each station in your dataframe, fetch is_day from Open-Meteo and merge it on
    the datetime column nearest_hour.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain 'station' and a datetime column 'nearest_hour'
    stations : dict
        {station_code: {"latitude": float, "longitude": float}, ...}
    timezone : str
        Open-Meteo timezone for returned hourly data (default Europe/London)

    Returns
    -------
    pd.DataFrame
        Original df with an added 'is_day' column (int8)
    """
    out = []
    # Ensure it's datetime (user said it already is, but let's be safe)
    df = df.copy()
    df["nearest_hour"] = pd.to_datetime(df["nearest_hour"]).dt.tz_convert("Europe/London")

    for station, coords in stations.items():
        sub = df[df["station"] == station].copy()
        if sub.empty:
            continue

        start_date = sub["nearest_hour"].min().date().isoformat()
        end_date   = sub["nearest_hour"].max().date().isoformat()

        is_day_df = fetch_is_day(
            coords["latitude"],
            coords["longitude"],
            start_date,
            end_date,
            timezone=timezone
        )

        # Exact join on the (local) hour
        merged = sub.merge(
            is_day_df,
            left_on="nearest_hour",
            right_on="time",
            how="left"
        ).drop(columns=["time"])

        out.append(merged)

    return pd.concat(out, ignore_index=True) if out else df


# ---------- example usage ----------

stations = load_json_to_dict('liv', 'eus')
df = pd.read_csv(INDIVIDUAL_DIRECTIONS / "liv_eus_final.csv", parse_dates=["nearest_hour"])

df_with_is_day = add_is_day_using_nearest_hour(df, stations)
df_with_is_day.to_csv(INDIVIDUAL_DIRECTIONS / 'liv_eus_final.csv', index=False)

print(df_with_is_day[['nearest_hour', 'station', 'is_day']].head(20))



