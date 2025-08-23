from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_helpers import get_all_stations, map_station_to_id, get_all_routes, get_all_route_stations, get_all_stoppings, map_route_to_id
from web_app.config import WEBAPP_DB, STOPPINGS_DATA
from init_db import Station, TrainStopping, Route, RouteStation
import pandas as pd

if __name__ == '__main__':

    #get db filepath as string
    db_filepath_string = str(WEBAPP_DB)

    #create session
    engine = create_engine(f'sqlite:///{db_filepath_string}')
    Session = sessionmaker(bind=engine)
    session = Session()

    #clear tables to avoid duplicates on rerun
    session.query(Station).delete()
    session.query(TrainStopping).delete()
    session.query(Route).delete()
    session.query(RouteStation).delete()

    #get all stations and add to the stations table
    stations = get_all_stations()
    session.add_all(stations)
    session.flush()
    print(f'Added {len(stations)} stations to the Station table')

    #map station codes to their id using the station table
    code_id_dict = map_station_to_id(session)

    #read stoppings dataframe to dataframe
    stoppings_df = pd.read_csv(STOPPINGS_DATA, parse_dates=['date_x', 'scheduled_time', 'actual_time'])

    #add routes and get ids
    routes = get_all_routes(stoppings_df)
    session.add_all(routes)
    session.flush()
    print(f'Added {len(routes)} routes to the Route table')

    #map ids
    route_id_dict = map_route_to_id(session)

    #get route_stations
    route_stations = get_all_route_stations(stoppings_df, code_id_dict, route_id_dict)
    session.add_all(route_stations)
    session.flush()
    print(f'Added {len(route_stations)} route_station rows to the Route_Station table')

    #add stoppings
    stoppings = get_all_stoppings(stoppings_df, code_id_dict, route_id_dict)
    batch_size = 100_000
    for i in range(0, len(stoppings), batch_size):
        session.bulk_save_objects(stoppings[i:i+batch_size])
        session.commit()
        print(f"Committed rows {i} to {i+batch_size} to train stopping table")

    #close session
    session.commit()
    session.close()

    print('Database populated')


    
