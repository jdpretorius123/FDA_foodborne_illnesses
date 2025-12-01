"""Function tests for data_wrangler.py.

This module allows the user to perform basic tests on the
methods in the Data_wrangler class.

This script requires data_wrangler.py and contains the following
functions.

Functions
---------
    test_path() -> None:
        Test Data_wrangler's ability to cache a JSON file path
        when initializing an instance of the class, and set a
        JSON file path

    test_is_data_unpacked() -> None:
        Test Data_wrangler's ability to set the flag to indicate
        whether the scraped data has been unpacked

    test_keys() -> None:
        Test Data_wrangler's ability to cache and set the keys 
        of the dictionary holding the data after its been scraped 
        and unpacked

    test_tables() -> None:
        Test Data_wrangler's ability to cache and set the tables 
        of the dictionary holding the data after its been scraped 
        and unpacked

    test_num_entries() -> None:
        Test Data_wrangler's ability to cache and set the number 
        of entries in the dictionary holding the data after its
        been scraped and unpacked

    test_scraped data() -> None:
        Test Data_wrangler's ability to cache and set the dictionary
        holding the data after its been scraped and unpacked

    test_read_file() -> None:
        Test Data_wrangler's ability to read the JSON file located at
        the end of the cached path

    test_unpack_data() -> None:
        Test Data_wrangler's ability to unpack and restructure the
        JSON data
"""