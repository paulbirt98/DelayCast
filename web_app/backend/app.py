from flask import Flask, render_template, request
import os
from dotenv import load_dotenv
from flask import jsonify
from web_app.config import METADATA_DIR
import json
import pandas as pd
from pathlib import Path


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
                    station_name = name
                    code = details.get('station_code')
                    latitude = details.get('latitude')
                    longitude = details.get('longitude')
                    if latitude is None or longitude is None:
                        continue
                    if name not in all_stations:
                        all_stations[name] = (code, latitude, longitude) 

    return jsonify(all_stations)

if __name__ == '__main__':
    app.run(debug=True)