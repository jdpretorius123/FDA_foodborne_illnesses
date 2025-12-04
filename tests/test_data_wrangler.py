"""Function tests for data_wrangler.py.

This module allows the user to perform basic tests on the
methods in the Data_wrangler class.

This script requires data_wrangler.py and contains the following
functions.

Functions
---------
    test_path() -> None:
        Test Data_Wrangler's ability to cache a JSON file path
        when initializing an instance of the class, and set a
        JSON file path.

    test_is_data_unpacked() -> None:
        Test Data_Wrangler's ability to set the flag to indicate
        whether the scraped data has been unpacked.

    test_keys() -> None:
        Test Data_Wrangler's ability to set the keys of the dictionary
        holding the data after its been scraped and unpacked.

    test_tables() -> None:
        Test Data_Wrangler's ability to set the tables of the dictionary
        holding the data after its been scraped and unpacked.

    test_num_entries() -> None:
        Test Data_Wrangler's ability to set the number 
        of entries in the dictionary holding the data after its
        been scraped and unpacked.

    test_scraped data() -> None:
        Test Data_Wrangler's ability to set the dictionary
        holding the data after its been scraped and unpacked.

    test_read_file() -> None:
        Test Data_Wrangler's ability to read the JSON file located at
        the end of the cached path.

    test_unpack_data() -> None:
        Test Data_Wrangler's ability to unpack and restructure the
        JSON data.

    test_metadata() -> None:
        Test Data_Wrangler's ability to print metadata after the data
        has been unpacked.
"""

# Loading dependencies

# Importing the Data_Wrangler class
from calendar import day_abbr
import sys
sys.path.append('./')
from classes.data_wrangler import Data_Wrangler

# pandas is needed for type checking
import pandas as pd

# io and contextlib allow print information to be checked 
import io
from io import StringIO
from contextlib import redirect_stdout

# re is needed for regex expressions
import re

def test_path() -> None:
    """
    Test path.

    Test Data_Wrangler's ability to cache, get, and set a
    JSON file path when initializing an instance of the class.
    """
    # Defining a path to the data
    true_path: str = './data/fda_investigations_data.json'

    # Creating an instance of the Data_Wrangler class
    data_wrangler: Data_Wrangler = Data_Wrangler(path = true_path)
    
    # Testing path caching
    assert data_wrangler.path == true_path

    # Testing path setting
    fake_path: str = './data/fake_file.json'
    data_wrangler.path = fake_path

    assert data_wrangler.path == fake_path

def test_is_data_unpacked() -> None:
    """
    Test is_data_unpacked.

    Test Data_Wrangler's ability to set the initial flag value and
    to indicate whether the scraped data has been unpacked.
    """
    # Defining the path to the data
    path: str = './data/fda_investigations_data.json'

    # Creating an instance of the Data_wrangler class
    data_wrangler: Data_Wrangler = Data_Wrangler(path = path)

    # Defining flag states
    not_unpacked: int = 0
    unpacked: int = 1

    # Testing the initial flag value
    assert data_wrangler.is_data_unpacked == not_unpacked

    # Unpacking the data
    data_wrangler.unpack_data()

    # Testing the flag's ability to indicate the data has been
    # unpacked
    assert data_wrangler.is_data_unpacked == unpacked

def test_keys() -> None:
    """
    Test keys.

    Test Data_Wrangler's ability to set the keys of the dictionary
    holding the data after its been scraped and unpacked.
    """
    # Defining the path to the data
    path: str = './data/fda_investigations_data.json'

    # Creating an instance of the Data_wrangler class
    data_wrangler: Data_Wrangler = Data_Wrangler(path = path)

    # Defining the keys of the dictionary holding the unpacked data
    true_keys: list[str] = [
        'Active Investigations',
        'Closed Investigations 2025',
        'Closed Investigations 2024',
        'Closed Investigations 2023',
        'Closed Investigations 2022',
        'Closed Investigations 2021',
        'Closed Investigations 2020'
    ]

    # Unpacking the data
    data_wrangler.unpack_data()

    # Testing the keys setting
    for idx in range(len(true_keys)):
        assert data_wrangler.keys[idx] == true_keys[idx]

def test_tables() -> None:
    """
    Test tables.

    Test Data_Wrangler's ability to set the values (tables) of the
    dictionary holding the data after its been scraped and unpacked.
    """
    # Defining the path to the data
    path: str = './data/fda_investigations_data.json'

    # Creating an instance of the Data_wrangler class
    data_wrangler: Data_Wrangler = Data_Wrangler(path = path)

    # Unpacking the data
    data_wrangler.unpack_data()

    # Testing the tables setting
    for table in data_wrangler.tables:
        assert isinstance(table, pd.DataFrame)
        assert not table.empty

def test_num_entries() -> None:
    """
    Test num_entries.

    Test Data_Wrangler's ability to set the number of entries in
    the dictionary holding the data after its been scraped and unpacked.
    """
    # Defining the path to the data
    path: str = './data/fda_investigations_data.json'

    # Creating an instance of the Data_wrangler class
    data_wrangler: Data_Wrangler = Data_Wrangler(path = path)

    # Defining the true number of entries in the dictionary
    true_num_entries: int = 7

    # Unpacking the data
    data_wrangler.unpack_data()

    # Testing the num_entries setting
    assert data_wrangler.num_entries == true_num_entries

def test_scraped_data() -> None:
    """
    Test scraped_data.

    Test Data_Wrangler's ability to set the dictionary
    holding the data after its been scraped and unpacked.
    """
    # Defining the path to the data
    path: str = './data/fda_investigations_data.json'

    # Creating an instance of the Data_wrangler class
    data_wrangler: Data_Wrangler = Data_Wrangler(path = path)

    # Unpacking the data
    data_wrangler.unpack_data()

    # Defining the keys of the dictionary holding the unpacked data
    true_keys: list[str] = [
        'Active Investigations',
        'Closed Investigations 2025',
        'Closed Investigations 2024',
        'Closed Investigations 2023',
        'Closed Investigations 2022',
        'Closed Investigations 2021',
        'Closed Investigations 2020'
    ]

    # Testing the scraped_data setting
    assert isinstance(data_wrangler.scraped_data, dict)

    # Testing the keys of the dictionary
    for idx in range(len(true_keys)):
        assert data_wrangler.keys[idx] == true_keys[idx]

    # Testing the values (tables) of the dictionary
    for table in data_wrangler.tables:
        assert isinstance(table, pd.DataFrame)
        assert not table.empty

def test_read_file() -> None:
    """
    Test read_file.

    Test Data_Wrangler's ability to read the JSON file located at
    the end of the cached path.
    """
    # Defining the path to the data
    path: str = './data/fda_investigations_data.json'

    # Creating an instance of the Data_wrangler class
    data_wrangler: Data_Wrangler = Data_Wrangler(path = path)

    # Defining the true column names of the read file
    true_colnames: list[str] = ['tableName', 'data']

    # Testing the file at the end of the path is read
    df_read_file: pd.DataFrame = data_wrangler._read_file()
    assert isinstance(df_read_file, pd.DataFrame)
    assert not df_read_file.empty

    # Defining the column names of the read file
    colnames: list[str] = df_read_file.columns.to_list()

    # Testing the validity of the column names
    for idx in range(len(true_colnames)):
        assert colnames[idx] == true_colnames[idx]

def test_metadata() -> None:
    """
    Test metadata.

    Test Data_Wrangler's ability to print correct metadata after the
    data has been unpacked.
    """
    # Defining the path to the data
    path: str = './data/fda_investigations_data.json'

    # Creating an instance of the Data_Wrangler class
    data_wrangler: Data_Wrangler = Data_Wrangler(path = path)
    
    # Unpacking the data
    data_wrangler.unpack_data()

    # Initializing StringIO to catch print statements
    f: io.StringIO = io.StringIO()
    with redirect_stdout(f):
        data_wrangler.metadata()

    # Getting the printed output when the data has been unpacked
    output: str = f.getvalue()

    # Defining the expected output
    expected_output: str = """
    The attributes of the unnested data include:
    structure
    keys
    tables
    num_entires
    scraped_data

    The scraped data is structured as a dictionary
    (dict[str, pd.DataFrame]) with 7 entries. Each
    entry key is a table name as a string and each
    value is the table data as a pd.DataFrame.

    Keys:
    Active Investigations
    Closed Investigations 2025
    Closed Investigations 2024
    Closed Investigations 2023
    Closed Investigations 2022
    Closed Investigations 2021
    Closed Investigations 2020

    Number of entries: 7

    Here is the first table:
    Columns:
    DatePosted
    Reference#
    PathogenorCause ofIllness
    Product(s)Linked toIllnesses(if any)
    TotalCaseCount
    InvestigationStatus
    Outbreak/EventStatus
    RecallInitiated
    FDATracebackInitiated
    FDAInspectionInitiated
    FDASamplingInitiated
    """

    # Splitting the expected output across all newline character instances
    expected_output_list: list[str] = expected_output.split('\n')
    
    # Stripping all unnecessary white space from the expected output lines
    expected_output_list = [line.rstrip().lstrip() for line in expected_output_list]

    # Joining the expected output elements by the newline character 
    expected_output = '\n'.join(expected_output_list)

    # Removing only the newline character at the beginning of the expected output
    expected_output = expected_output.lstrip('\n')

    # Testing the expected output of metadata when the data has been
    # unpacked
    assert expected_output in output

    # Creating another instance of the Data_Wrangler class
    new_data_wrangler: Data_Wrangler = Data_Wrangler(path = path)

    # Initializing another StringIO to catch print statements indicating the data
    # has not been unpacked
    unpacked_f: io.StringIO = io.StringIO()
    with redirect_stdout(unpacked_f):
        new_data_wrangler.metadata()

    # Getting the printed output when the data hasn't been unpacked 
    unpacked_output: str = unpacked_f.getvalue()

    # Testing the expected output of metadata when the data has not been
    # unpacked
    assert 'Please unpack the data before trying to read the metadata.' in unpacked_output


    