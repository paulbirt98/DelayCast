import streamlit

def map_name_to_details(station_data):
    """
    
    """
    name_to_code = {}

    for station in station_data:
        name = station.get("station_name")
        code = station.get("station_code")
        longitude = station.get("longitude")
        latitude = station.get("latitude")

        #avoid dupes
        if (name and code) and name not in name_to_code:
            name_to_code[name] = {'station_code': code, 'longitude': float(longitude),'latitude': float(latitude)}

    return name_to_code