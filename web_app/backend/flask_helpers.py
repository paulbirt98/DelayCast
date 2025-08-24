from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from web_app.config import WEBAPP_DB
from web_app.database.db_utils.init_db import HourlyForecast, Station

def get_most_recent_forecast(session, station, now):
    """
    
    """
    most_recent = (
        session.query(HourlyForecast.timestamp_utc).filter(
            HourlyForecast.station_id == station.station_id,
            HourlyForecast.timestamp_utc <= now
        ).order_by(HourlyForecast.timestamp_utc.desc())
        .limit(1)
        .scalar()
    )

    return most_recent
