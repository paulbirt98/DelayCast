import json
from shapely.geometry import LineString
import folium
import pandas as pd
import geopandas as gpd


def build_base_map():
    """
    
    """
    m = folium.Map(location=[54.5, -3], zoom_start=6)
    
    folium.TileLayer(
        tiles="https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png",
        attr='&copy; <a href="https://www.stadiamaps.com/">Stadia Maps</a> '
             '&copy; <a href="https://openmaptiles.org/">OpenMapTiles</a> '
             '&copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors',
        name="Stadia.AlidadeSmooth",
        max_zoom=20
    ).add_to(m)

    return m

def add_stations(m, all_stations, colour='#000000', size=3):
    """
    
    """

    #add all stations to map
    for station, (code, latitude, longitude) in all_stations.items():
        folium.CircleMarker(
            location=[latitude, longitude],
            radius=size,
            popup=f'{station} ({code})',
            color=colour,
            fill=True, fill_opacity=0.8, 
        ).add_to(m)

def add_lines(m, metadata):
    """
    
    """
    #get route line visuals
    core = pd.read_csv(METADATA / 'nf_core.csv')

    geo_data = []

    for elr, group in core.groupby('elr'):
        group_sorted = group.sort_values('total_yards')
        coords = list(zip(group_sorted['longitude'], group_sorted['latitude']))
        geo_data.append({'elr': elr, 'geometry': LineString(coords)})

    #get geo dataframe
    geo_df = gpd.GeoDataFrame(geo_data, crs='EPSG:4326')

    geo_df.to_file('studied_routes.geojson', driver="GeoJSON")