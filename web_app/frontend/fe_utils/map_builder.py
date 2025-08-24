import pydeck as pdk

def build_lines(lines_geojson):
    """
    
    """
    train_lines = []

    for feature in lines_geojson.get("features", []):
        geometry = feature.get("geometry", {})
        properties = feature.get("properties", {})
        coords = geometry.get("coordinates", [])
        colour = properties.get("colour", [0, 0, 0])
        operator = properties.get("operator", "")
        elr = properties.get("elr", "")

        train_lines.append({
            "path": coords,
            "operator": operator,
            "elr": elr,
            "colour": colour
        })
    
    return train_lines

def build_map(station_data, lines_geojson, centre=(-3.0, 54.5), zoom=5.8, line_widths=15):
    """
    
    """

    #make layer for the lines
    lines_layer = pdk.Layer(
        'PathLayer',
        data=lines_geojson,
        get_path='path',
        get_width=line_widths,
        get_color='colour',
        width_min_pixels=5,
        pickable=False,
        opacity=0.9,
    )

    #make station layer
    station_colour = [255, 255, 255]

    stations_layer = pdk.Layer(
        "ScatterplotLayer",
        data=station_data,
        get_position=["longitude", "latitude"],
        get_fill_color=station_colour,
        get_line_color=[0, 0, 0],
        line_width_min_pixels=2,
        stroked=True,        
        radius_scale=10,
        radius_min_pixels=5,          
        radius_max_pixels=1000, 
        pickable=True,
        opacity=0.9,
    )

    #call weather

    #displayed details on hover
    tooltip = {
        "html": 
            """
            <div style="display:flex;align-items:center;gap:8px;">
                <div><b>{station_name}</b> ({station_code})</div>
                <img src="{icon}" width="40" height="40" style="display:block"/>
            </div>
            """,
        "style": {"backgroundColor": "white", "color": "black"}
    }

    #centre on UK
    view_state = pdk.ViewState(
        longitude=centre[0], latitude=centre[1], zoom=zoom, pitch=0, bearing=0
    )

    #join layers
    map = pdk.Deck(
        layers=[lines_layer, stations_layer],
        initial_view_state=view_state,
        map_provider='carto',
        map_style='https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
        tooltip=tooltip
    )

    return map