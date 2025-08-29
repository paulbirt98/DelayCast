from datetime import timedelta
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from web_app.config import WEBAPP_DB
from web_app.database.db_utils.init_db import HourlyForecast, Station

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
