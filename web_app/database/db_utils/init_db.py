from sqlalchemy import UniqueConstraint, create_engine, Column, String, Float, Boolean, DateTime, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from web_app.config import WEBAPP_DB

Base = declarative_base()

#create a stations table
class Station(Base):
    __tablename__ = 'station'

    #cplumns
    station_id = Column(Integer, primary_key=True, autoincrement=True)
    station_code = Column(String(3), nullable=False, unique=True)
    station_name = Column(String(50), nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)

    #fk relationships
    forecasts = relationship("HourlyForecast", back_populates="station")

#hourly forecasts table
class HourlyForecast(Base):
    __tablename__ = 'hourly_forecast'
    __table_args__ = (
        UniqueConstraint('station_id', 'timestamp_utc', name='uix_station_timestamp'),
    )

    forecast_id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey('station.station_id'), nullable=False)
    timestamp_utc = Column(DateTime, nullable=False)

    weather_code = Column(Integer)
    temp_2m = Column(Float)
    relative_humidity = Column(Float)
    rain = Column(Float)
    gusts = Column(Float)
    snow_depth = Column(Float)
    surface_pressure = Column(Float)
    is_day = Column(Boolean)

    station = relationship("Station", back_populates="forecasts")

#route table
class Route(Base):
    __tablename__ = 'route'

    route_id = Column(Integer, primary_key=True, autoincrement=True)
    route_name = Column(String(100), nullable=False)
    operator = Column(String(100))

    stations = relationship("RouteStation", back_populates="route")


#route - station table
class RouteStation(Base):
    __tablename__ = 'route_station'

    route_id = Column(Integer, ForeignKey('route.route_id'), primary_key=True)
    station_id = Column(Integer, ForeignKey('station.station_id'), primary_key=True)
    is_first_station = Column(Boolean)
    is_terminus = Column(Boolean)

    route = relationship("Route", back_populates="stations")
    station = relationship("Station")

#create the db file in the database folder
if __name__ == '__main__':
    filepath  = WEBAPP_DB
    engine = create_engine(f'sqlite:///{WEBAPP_DB}')
    Base.metadata.create_all(engine)
    print('Web app database initialised')