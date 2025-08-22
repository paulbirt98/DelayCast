from flask import Flask, request
import os
from dotenv import load_dotenv
from flask import jsonify
from web_app.config import METADATA_DIR, WANTED_ELRS
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

app = Flask(__name__)

#get secret key from .env
load_dotenv()
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")

@app.route('/api/message')
def api_message():
    return jsonify({'message': 'Hello from Flask!'})

@app.route('/all_stations')
def all_stations():

    #placeholder
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

                    all_stations.append({
                        'station_name': name,
                        'station_code': code,
                        'longitude': float(longitude),
                        'latitude': float(latitude),
                    })

    return jsonify(all_stations)

@app.route('/line_details')
def line_details():

    #get netwrok fusion file path and read to dataframe
    nf_filepath = METADATA_DIR / 'nf_core.csv'
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

@app.route('/station_info')
def station_info():

    station_code = request.args.get('station_code', '').upper()

    if not station_code:
        return jsonify({'Error': 'No station code provided in request'}), 400
    
    #iterate through and pull only the relevant station details
    for file in METADATA_DIR.iterdir():
        if file.suffix.lower() == '.json':
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)

                for name, details in data.items():
                    if details.get('station_code') == station_code:
                        code = details.get('station_code')
                        longitude = details.get('longitude')
                        latitude = details.get('latitude')

                        station_info = {
                        'station_name': name,
                        'station_code': code,
                        'longitude': float(longitude),
                        'latitude': float(latitude),
                        }

                        return jsonify(station_info)
    
    return jsonify({'Error': 'No station found with code provided'}), 401


if __name__ == '__main__':
    app.run(debug=True)