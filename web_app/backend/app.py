from flask import Flask, request
import os
from dotenv import load_dotenv
from flask import jsonify
from web_app.config import METADATA_DIR, NF_CORE, WANTED_ELRS, WEBAPP_DB, FORECAST_LENGTH
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
from web_app.database.db_utils.init_db import Station, Route, RouteStation, HourlyForecast
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
from datetime import date, datetime, timedelta
from flask_helpers import get_most_recent_forecast

app = Flask(__name__)

#get secret key from .env
load_dotenv()
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")

#connect to db
db_filepath_string = str(WEBAPP_DB)
engine = create_engine(f'sqlite:///{db_filepath_string}')
Session = sessionmaker(bind=engine)

@app.route('/all_stations')
def all_stations():

    all_stations = []

    session = Session()
    stations = session.query(Station).all()
    session.close()

    for station in stations:
        all_stations.append({
            "station_name": station.station_name,
            "station_code": station.station_code,
            "longitude": station.longitude,
            "latitude": station.latitude
        })

    return jsonify(all_stations)

@app.route('/station_info')
def station_info():

    station_code = request.args.get('station_code', '').upper()

    if not station_code:
        return jsonify({'Error': 'No station code provided in request'}), 400
    
    #query db for the requested station info
    session = Session()
    station = session.query(Station).filter_by(station_code=station_code).first()
    session.close()
    
    if station is None:
        return jsonify({'Error': 'No station found with code provided'}), 404
    
    station_info = {
        "station_name": station.station_name,
        "station_code": station.station_code,
        "longitude": station.longitude,
        "latitude": station.latitude
    }

    return jsonify(station_info)

@app.route('/line_details')
def line_details():

    #get netwrok fusion file path and read to dataframe
    nf_filepath = NF_CORE
    lines_df = pd.read_csv(nf_filepath)

    line_details = []

    #loop through the desired elrs and gather coords
    for toc, attributes in WANTED_ELRS.items():
        colour = attributes.get('colour')
        for elr in attributes.get('lines'):
            elr_df = lines_df[lines_df['elr'] == elr].sort_values('total_yards')
            coords = elr_df[['longitude', 'latitude']].values.tolist()
            
            line = LineString(coords)
            line_details.append({
                'type': 'Feature',
                'geometry': gpd.GeoSeries([line]).__geo_interface__['features'][0]['geometry'],
                'properties': {'elr': elr, 'operator': toc, 'colour': colour}
            })
    
    geojson = {
        "type": 'FeatureCollection',
        "features": line_details
    }

    return jsonify(geojson)

@app.route('/get_location_forecast')
def get_location_forecast():

    #get todays date
    num_days = FORECAST_LENGTH

    station_code = request.args.get('station_code', '').upper()

    if not station_code:
        session.close()
        return jsonify({'Error': 'No location provided in request'}), 400

    #query db for the requested station info
    session = Session()

    #match the station code
    station = session.query(Station).filter_by(station_code=station_code).first()
    if not station:
            session.close()
            return jsonify({'error': f'Station {station_code} not found'}), 404

    #get the current time
    now = datetime.now()

    #get the most recent hourly forecast
    recent_forecast = get_most_recent_forecast(session, station, now)

    if recent_forecast is None:
        return jsonify({'Error': f'No forecasts yet for {station_code}'}), 404

    #get last hour of forecast period
    forecast_limit = recent_forecast + timedelta(days=num_days)

    #fetch the whole forecast from db
    entire_forecast = (
        session.query(HourlyForecast)
            .filter(
                HourlyForecast.station_id == station.station_id,
                HourlyForecast.timestamp_utc >= recent_forecast,
                HourlyForecast.timestamp_utc <= forecast_limit
            )
            .order_by(HourlyForecast.timestamp_utc.asc())
            .all()
    )

    #build object to return
    forecast_object = {
        'station_code': station.station_code,
        'hourly_forecasts': [
            {
                'timestamp': hour.timestamp_utc.isoformat(),
                'weather_code': hour.weather_code,
                'temp_2m': hour.temp_2m,
                'humidity': hour.relative_humidity,
                'rain': hour.rain,
                'gusts': hour.gusts,
                'snow_depth': hour.snow_depth,
                'surface_pressure': hour.surface_pressure,
                'is_day': hour.is_day
            } for hour in entire_forecast
        ]
    }

    session.close()

    return jsonify(forecast_object)

@app.route('/current_weather')
def get_current_weather():
    
    session = Session()

    now = datetime.now()

    


if __name__ == '__main__':
    app.run(debug=True)