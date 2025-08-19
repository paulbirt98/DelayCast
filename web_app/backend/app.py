from flask import Flask, render_template, request
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

@app.route('/station_details')
def station_details():

    #placeholder
    all_stations = {}

    #for all station json files add all stations to 'all_stations'
    for file in METADATA_DIR.iterdir():
        if file.suffix.lower() == '.json':
            with open(file, "r", encoding="utf-8") as f: #read the json file
                data = json.load(f)

                for name, details in data.items():
                    code = details.get('station_code')
                    latitude = details.get('latitude')
                    longitude = details.get('longitude')
                    if latitude is None or longitude is None:
                        continue
                    if name not in all_stations:
                        all_stations[name] = (code, latitude, longitude) 

    return jsonify(all_stations)

@app.route('/line_coords')
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


if __name__ == '__main__':
    app.run(debug=True)