import requests
from io import StringIO
import pandas as pd
import logging
import sqlite3

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_connection(fname):
    """Create and return a database connection to a single SQLite database."""
    db_path = f"{fname}.db"
    try:
        conn = sqlite3.connect(db_path)
        logging.info(f"Database connected successfully at {db_path}")
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        return None
    return conn

def download_and_process_data(url,sep, encoding='utf-8', low_memory=False):
    """Download and process CSV data from a URL."""
    response = requests.get(url)
    if response.status_code == 200: # The request was successfully received, understood, and processed by the server
        data = StringIO(response.text)
        df = pd.read_csv(data, sep=sep, encoding=encoding, low_memory=low_memory)
        # Handle null values appropriately
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)  # Or use another placeholder
        df.fillna('Default Value', inplace=True)
        return df
    else:
        logging.error(f"Failed to download data: HTTP {response.status_code}")
        return None

def process_data(df, table_name,dbname, column_mapping):
    """Create table, and load data into the specified table within the database.
    Optionally, manipulate the DataFrame columns based on `column_mapping`."""
    conn = create_connection(dbname)
    if conn and df is not None:
        try:
         
            # Rename columns based on provided mapping
            if column_mapping:
                df = df.rename(columns=column_mapping)  
          
            # Retain only the columns that have been renamed
            columns_to_keep = list(column_mapping.values())
            df = df[columns_to_keep]

            if df.columns.duplicated().any():
               logging.error("Duplicate column names found after renaming: Please check the column mapping.")
               return  # Add this to prevent attempting to load into SQL with duplicate columns
            
            # Load data
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.commit()
            logging.info(f"Data loaded successfully into {table_name}.")
        except Exception as e:
            logging.error(f"Failed to process data for {table_name}: {e}")
        finally:
            conn.close()
    else:
        logging.error("Failed to setup database or download data.")


# Process and load first dataset
url1 = 'https://www.eea.europa.eu/data-and-maps/data/climate-change-mitigation-policies-and-measures-1/pam-table/climate-change-mitigation-policies-and-3/download.csv'
df1 = download_and_process_data(url1, ',', encoding='utf-8')
if df1 is not None:
    column_mapping1 = {
        'Country:text' : 'Country', 
        'Name_of_policy_or_measure:text': 'Name_of_policy_or_measure',
        'Single_policy_or_measure__or_group_of_measures:text': 'Single_policy_or_measure__or_group_of_measures',
        'Status_of_implementation_clean:text': 'Status',
        'Implementation_period_start_clean:text': 'start_clean',
        'Is_the_policy_or_measure_related_to_a_Union_policy__clean:text': 'Is_the_policy_or_measure_related_to_a_Union_policy__clean',
        'GHG_s__affected:text': 'GHG_s__affected'
    }
    process_data(df1, 'climate_change_policies','climate_change_policies', column_mapping1)

# Process and load second dataset
url2 = 'https://opendata.edf.fr/api/explore/v2.1/catalog/datasets/enquete-dopinion-internationale-sur-le-changement-climatique-obscop/exports/csv'
df2 = download_and_process_data(url2,';', encoding='utf-8')
if df2 is not None:
    column_mapping2 = {
        'annee' : 'Year', 
        'population': 'Country',
        'details_of_the_question': 'Question in Detail',
        'reponse': 'Reponse',
        'answer': 'Answer',
        'valeur': 'Valeur',
        'value': 'value'
    }
    process_data(df2, 'climate_change_survey','climate_change_survey', column_mapping2)
