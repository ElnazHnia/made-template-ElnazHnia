import requests
from io import StringIO
from sqlalchemy import create_engine, text
import pandas as pd
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Setup the database connection
def engine_fun(fname):
    dbname= f"sqlite:///{fname}.db"
    database_url = os.getenv("DATABASE_URL", dbname)
    engine = create_engine(database_url)
    try:
        # Use a context manager to ensure the connection is closed after use
        with engine.connect() as connection:
            # Use the text() function to ensure the SQL command is correctly interpreted
            connection.execute(text("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT);"))
            # print("Table created successfully.")
            return engine
            # print("Current working directory:", os.getcwd())
    except Exception as e:
        print(f"An error occurred: {e}")

# Function to drop a table if it exists
def drop_table(table_name, engine):
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
        # logging.info(f"Table {table_name} has been dropped.")
    except Exception as e:
        logging.error(f"Error dropping {table_name}: %s", e)



# Function to load data into SQL
def load_data_to_sql(df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Data loaded successfully into {table_name}.")
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: %s", e)

# Download and process first CSV file: climate_change_policies
try:
    
    df1 = pd.read_csv('https://www.eea.europa.eu/data-and-maps/data/climate-change-mitigation-policies-and-measures-1/pam-table/climate-change-mitigation-policies-and-3/download.csv', 
                       sep=',', encoding='utf-8')
    
    # Handle null values appropriately
    numeric_columns = df1.select_dtypes(include=['float64', 'int64']).columns
    df1[numeric_columns] = df1[numeric_columns].fillna(0)  # Or use another placeholder
    #  the database connection
    engine1 = engine_fun('climate_change_policies')
    # Example of how to drop the tables
    drop_table('climate_change_policies', engine1)
    # Select useful columns
    df1 = df1[['Country:text', 'Name_of_policy_or_measure:text','Single_policy_or_measure__or_group_of_measures:text','Status_of_implementation_clean:text','Implementation_period_start_clean:text','Is_the_policy_or_measure_related_to_a_Union_policy__clean:text','GHG_s__affected:text']]
    # Rename them
    if {'Country:text', 'Name_of_policy_or_measure:text','Single_policy_or_measure__or_group_of_measures:text','Status_of_implementation_clean:text','Implementation_period_start_clean:text','Is_the_policy_or_measure_related_to_a_Union_policy__clean:text','GHG_s__affected:text'}.issubset(df1.columns):
        df1 = df1.rename(columns={
           'Country:text' : 'Country', 
           'Name_of_policy_or_measure:text': 'Name_of_policy_or_measure',
           'Single_policy_or_measure__or_group_of_measures:text': 'Single_policy_or_measure__or_group_of_measures',
           'Status_of_implementation_clean:text': 'Status',
           'Implementation_period_start_clean:text': 'start_clean',
           'Is_the_policy_or_measure_related_to_a_Union_policy__clean:text': 'Is_the_policy_or_measure_related_to_a_Union_policy__clean',
           'GHG_s__affected:text': 'GHG_s__affected'
        })
    df1.to_sql('climate_change_policies', con=engine1, if_exists='replace', index=False)
except Exception as e:
    logging.error("Failed to process climate_change_policies: %s", e)

# Download and process second CSV file: climate_change_survey
try:
    response = requests.get('https://opendata.edf.fr/api/explore/v2.1/catalog/datasets/enquete-dopinion-internationale-sur-le-changement-climatique-obscop/exports/csv')
    if response.status_code == 200:
        data = StringIO(response.text)
        # names=['Year', 'Country', 'Question','Reponse', 'Answer', 'Valeur','value']
        df2 = pd.read_csv(data, sep=';',  encoding='utf-8', low_memory=False)
         #  the database connection
        engine2 = engine_fun('climate_change_survey')
        # Example of how to drop the tables
        drop_table('climate_change_survey', engine2)
        # Handle null values appropriately
        numeric_columns = df2.select_dtypes(include=['float64', 'int64']).columns
        df2[numeric_columns] = df2[numeric_columns].fillna(0)  # Or use another placeholder
        df2.fillna('Default Value', inplace=True)
        # Select useful columns
        df2 = df2[['annee','population',  'details_of_the_question', 'reponse', 'answer', 'valeur', 'value']]
        # Rename them
        if {'annee','population',  'details_of_the_question', 'reponse', 'answer', 'valeur', 'value'}.issubset(df2.columns):
            df2 = df2.rename(columns={
            'annee' : 'Year', 
            'population': 'Country',
            'details_of_the_question': 'Question',
            'reponse': 'Reponse',
            'answer': 'Answer',
            'valeur': 'Valeur',
            'value': 'value'
            })
        df2.to_sql('climate_change_survey', con=engine2, if_exists='replace', index=False)
    else:
        logging.error("Failed to download climate_change_survey: HTTP %s", response.status_code)
except Exception as e:
    logging.error("Failed to process climate_change_survey: %s", e)

# Query the database and print results
try:
    
    result1 = pd.read_sql('SELECT * FROM climate_change_policies', con=engine1)
    # print(result1.head())
    result2 = pd.read_sql('SELECT * FROM climate_change_survey', con=engine2)
    # print(result2.head())
except Exception as e:
    logging.error("Error executing query: %s", e)