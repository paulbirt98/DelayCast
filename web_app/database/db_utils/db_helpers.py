import json
from shapely.geometry import LineString
from flask import jsonify
from web_app.config import METADATA_DIR, NF_CORE, WANTED_ELRS, STOPPINGS_DATA
from init_db import Station, Route, RouteStation
import pandas as pd
from tqdm import tqdm

def get_all_stations():
    """
    
    """
    all_stations = []
    existing = set()

    #for all station json files add all stations to 'all_stations'
    for file in METADATA_DIR.iterdir():
        if file.suffix.lower() == '.json':
            with open(file, "r", encoding="utf-8") as f: #read the json file
                data = json.load(f)

                for name, details in data.items():
                    code = details.get('station_code')
                    longitude = details.get('longitude')
                    latitude = details.get('latitude')
                    if latitude is None or longitude is None:
                        continue

                    #if its already been added via a different file skip it
                    if code in existing:
                        continue
                    existing.add(code)

                    all_stations.append(Station(
                        station_code=code,
                        station_name=name,
                        longitude=float(longitude),
                        latitude=float(latitude)
                    ))
    return all_stations

def map_station_to_id(session):
    """
    
    """
    stations = session.query(Station).all()

    station_code_id_dict = {station.station_code: station.station_id for station in stations}

    return station_code_id_dict

def get_all_routes(stoppings_df):
    """
    
    """
    all_routes = []
    existing = set()

    for _, row in stoppings_df[['route', 'toc']].drop_duplicates().iterrows():
        pair = (row.route, row.toc)

        if pair not in existing:
            existing.add(pair)
            all_routes.append(Route(
                route_name=row.route,
                operator=row.toc
            ))

    return all_routes

def map_route_to_id(session):
    """
    
    """
    routes = session.query(Route).all()

    route_id_dict = {route.route_name: route.route_id for route in routes}

    return route_id_dict

def get_all_route_stations(stoppings_df, code_id_dict, route_id_dict):
    """
    
    """
    all_route_stations = []
    existing = set()

    for _, row in stoppings_df[['route', 'station', 'is_first_station', 'is_terminus']].drop_duplicates().iterrows():
        route_id = route_id_dict.get(row.route)
        station_id = code_id_dict.get(row.station)

        pair = (route_id, station_id)

        if pair not in existing:
            existing.add(pair)

            all_route_stations.append(RouteStation(
                route_id=route_id,
                station_id=station_id,
                is_first_station=bool(row.is_first_station),
                is_terminus = bool(row.is_terminus)
            ))

    return all_route_stations