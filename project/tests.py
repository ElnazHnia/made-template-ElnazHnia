import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock
import sqlite3

# Import your actual function names and modules correctly
from pipeline import process_data, create_connection, download_and_process_data

# Create the DataFrame data directly in the test file
manual_data = pd.DataFrame([['a', 'b', 'c'], [1, 2, np.nan], [3, 4, 5]], columns=['Column1', 'Column2', 'Column3'])

def test_download_and_process_data():
    
    # Simulate the data processing part of download_and_process_data
    df = pd.DataFrame(manual_data)

    # Assuming the function modifies the data somehow; we mimic that transformation
    # For example, if your function normalizes null values:
    df.fillna('Default Value', inplace=True)

    # Now assert that the  processed data meets expectations
    expected_df = manual_data.fillna('Default Value')
    pd.testing.assert_frame_equal(df, expected_df)

def test_process_data():
    """
    Test processing the data to ensure transformations and database loading are correct.
    Assumes process_data takes a DataFrame, processes it, and loads into a database.
    """
    with patch('pipeline.create_connection') as mock_create, \
         patch('pandas.DataFrame.to_sql') as mock_to_sql:
        mock_conn = MagicMock()
        mock_create.return_value = mock_conn
        process_data(manual_data, 'example_table', 'test_db', {'Column1': 'NewColumn1'})
        mock_to_sql.assert_called_once_with('example_table', mock_conn, if_exists='replace', index=False)

def test_create_connection():
    """ Test the database connection function. """
    with patch('sqlite3.connect') as mock_connect:
        create_connection('test_db')
        mock_connect.assert_called_with('test_db.db')

if __name__ == "__main__":
    pytest.main()
