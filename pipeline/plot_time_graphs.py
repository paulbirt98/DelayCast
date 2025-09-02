import pandas as pd
from pipeline_utils.visualisation_helpers import plot_delay_time_period
from pipeline_utils.config import INDIVIDUAL_ROUTES
from web_app.config import STATIC_FOLDER

time_periods = ['hour', 'day', 'month']

for directory in INDIVIDUAL_ROUTES.iterdir():
    for file in directory.iterdir():
        if file.name.endswith('_testing_data.csv'):
            try:
                df = pd.read_csv(file)
            except FileNotFoundError:
                print(f"Error: File not found")
                raise
            except pd.errors.ParserError:
                print(f"Error parsing file")
                raise
            except PermissionError:
                print(f"Permission Error with file. Ensure the file is not open elsewhere.")
                raise
            except Exception as e:
                print(f"Unexpected error reading file: {e}")

            for station in df['station'].dropna().unique():

                for time_period in time_periods:
                    time_graph_dir = STATIC_FOLDER / 'time_graphs'
                    time_graph_dir.mkdir(exist_ok=True, parents=True)
                    filepath = time_graph_dir / f"{station}_{time_period}_graph.svg"
                    
                    plot_delay_time_period(df, station, time_period, filepath=filepath)

                    print(f"Time graph for {station} by the {time_period} saved")