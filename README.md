# DelayCast

DelayCast is a user-facing predictive dashboard displaying the curretn and forecasted risk of delay (including the risk of moderate (>15mins) and severe (>30mins)) at differetn stations on 4 different routes across the UK. This dashboard uses predictions made using route-specific trained and tested RandomForest classification models with CallibratedClassifierCV's fitted to them.
The project as a whole consists of a machine learning pipeline from data fetching right through to model training, as well as the frontend, backend and database of the web application. It is initial exploratory opensource work that can be easily built upon due to the readability and maintatinability of the codebase. The backend is also designed in such a way that were the project to be deployed, its predictions could be easily integrated into other software projects via the API endpoints.

The submitted project includes all raw data fetched from the relevant APIs as well as the final data used to train the models - in CSV format - but all code required to replicate this data is also available should this be required**.

**The API calls fetched using the 'pipeline/add_new_route.py' script and its imported helper functions take a very long time to complete as the script loops through each hour of the day from 6am - 10pm for eaach day over a 10 year period (often 12 hours or more in total), so the Command Line Argument "--testing" can be entered as a flag to fetch just the most reent three days for testing purposes.

The model training scripts (e.g. 'pipeline/train_val_test_glq.py') can be run safely without changing the saved models used in the web application - these will only save if the Command Line Argument "--save_to_web" is included.

The SQLite database used 'web_app/database/web_app.db' should already be populated with the next 7 days worth of data (as of 3rd September 2025). This platform periodically deactivates accoutns after a period of inactivity - this shouldnt be an issue as I have logged in recently and will log in again in a week or two to ensure plenty of headroom - but if any issues arise due to this please reach out at 'paulbirt1998@gmail.com' or 'pbirt01@qub.ac.uk' (if uni account still accessible) and this can be easily remedied.

The .env file has been submitted including my email address and Rail Data Portal password - as these are required for the API auth headers. 

##FOLDER STRUCTURE (HIGH-LEVEL)##

DelayCast/
    data/
        metadata/
        processed/
        raw_api_responses/
        semi_processed/
    pipeline/
        pipeline_utils/
            config.py
            ...
        add_new_route.py
        ...
    web_app/
        backend/
            app.py
            flask_helpers
            model_inference.py
        database/
            db_utils/
                ...
            nf_core.csv
            station_baselines.json
            unified_routes.csv
            web_app.db
        frontend/
            fe_utils/
                ...
            static/
                ...
            router.py
            dasborad.py
            ...

##SET UP###

Download project using the link.

Ensure you're in the project root directory - this is vital to ensure the following Command Line prompts work correctly.

Set up and activate a Virtual Environment by running 'python -m venv .venv'.
Then run '.venv\Scripts\Activate' - if on a Windows Machine.

Install all dependencies by running 'pip install -r requirements.txt' , then 'pip install -e'.

Once this is all set up, to run a script ensure you include the parent directories - e.g. 'pipeline/preprocess.py'. With Command Line Arguments this would be as so: 'pipeline/preprocess.py --from_location glq --to_location inv

If there is an issue where the .db file need to be deleted and reinitiated, run the following commands in order to set this up again.

'web_app/database/db_utils/init_db.py'
'web_app/database/db_utils/populate_db_tables.py'
'web_app/database/db_utils/update_weather.py' #this can also be run at any time on its own, if the dashboard begins to show that there is no forecast data for any given day or hour

In order to start the flask backend run 'python -m web_app.backend.app'

Wait a few seconds once this is up and running to then run 'streamlit run web_app/frontend/router.py' which will start the front end.
This should automaticaly open a browser page on the DelayCast dasboard but it not please navigate to 'http://localhost:8501/'


