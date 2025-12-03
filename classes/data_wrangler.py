"""
Data_Wrangler reads in a JSON file and restructures its data into a
pandas dataframe. The data_wrangler class lacks generalizability 
at the moment, but regex and time can fix that.
"""

# Loading dependencies
import pandas as pd     # pandas enables data wrangling
from typing import Any  # Any serves as a type hint for objects with a complex structure

class Data_Wrangler:
    """
    Data_Wrangler contains a variety of functions to execute the following
    tasks:
        1. Takes a path to a data repository
        2. Reads in the JSON file at the end of the path
        3. Restructures the JSON data into a list of pandas dataframes

    Attributes
    ----------
    - path (str): path to repository with JSON data

    Methods
    -------
    - _read_file() -> pd.DataFrame
    - unpack_data() -> Dict[str, pd.DataFrame]
    """
    def __init__(self, path: str) -> None:
        """
        Initiate Data_wrangler class.
        
        Parameters
        ----------
        - path (str): path to repository with JSON data
        
        """
        self.path = path
        self._is_data_unpacked = 0
        self._structure = None
        self._keys = None
        self._tables = None
        self._num_entries = None
        self._scraped_data = None

    @property
    def path(self) -> str:
        """Path to respository with JSON data."""
        return self._path
    
    @path.setter
    def path(self, path: str) -> None:
        if isinstance(path, str):
            self._path = path
        else:
            raise TypeError('The path param must be a str.')
        
    @property
    def is_data_unpacked(self) -> int:
        """Flag indicating whether JSON data has been unpacked."""
        return self._is_data_unpacked
    
    @is_data_unpacked.setter
    def is_data_unpacked(self, flag: int) -> None:
        if isinstance(flag, int):
            if flag == 0 or flag == 1:
                self._is_data_unpacked = flag
        else:
            raise TypeError('The flag param must be an int and 0 or 1.')
        
    @property
    def structure(self) -> list[str]:
        """Description of the structure of the unpacked JSON data."""
        return self._structure
    
    @structure.setter
    def structure(self, structure: list[str]) -> None:
        if isinstance(structure, list):            
            for item in structure:
                if not isinstance(item, str):
                    raise TypeError('Each item in structure must be a str.')  
            self._structure = structure
        else:
            raise TypeError('The structure param must be a list.')
        
    @property
    def keys(self) -> list[str]:
        """Keys of the dictionary holding the unpacked JSON data."""
        return self._keys
    
    @keys.setter
    def keys(self, keys: list[str]) -> None:
        if isinstance(keys, list):            
            for item in keys:
                if not isinstance(item, str):
                    raise TypeError('Each item in keys must be a str.')  
            self._keys = keys
        else:
            raise TypeError('The keys param must be a list.')
        
    @property
    def tables(self) -> list[pd.DataFrame]:
        """Values (tables) of the dictionary holding the unpacked JSON data."""
        return self._tables
    
    @tables.setter
    def tables(self, tables: list[pd.DataFrame]) -> None:
        if isinstance(tables, list):            
            for item in tables:
                if not isinstance(item, pd.DataFrame):
                    raise TypeError('Each item in tables must be a pd.DataFrame.')  
            self._tables = tables
        else:
            raise TypeError('The tables param must be a list.')
        
    @property
    def num_entries(self) -> int:
        """Number of entries in the dictionary holding the unpacked JSON data."""
        return self._num_entries
    
    @num_entries.setter
    def num_entries(self, num_entries: int) -> None:
        if isinstance(num_entries, int):
            self._num_entries = num_entries
        else:
            raise TypeError('The num_entries param must be a int.')
        
    @property
    def scraped_data(self) -> dict[str, pd.DataFrame]:
        """Scraped unpacked JSON data."""
        return self._scraped_data
    
    @scraped_data.setter
    def scraped_data(self, scraped_data: dict[str, pd.DataFrame]) -> None:
        if isinstance(scraped_data, dict):            
            for value in scraped_data.values():
                if not isinstance(value, pd.DataFrame):
                    raise TypeError('Each item in tables must be a pd.DataFrame.')  
            self._scraped_data = scraped_data
        else:
            raise TypeError('The scraped_data param must be a dict.')
        
    def _read_file(self) -> pd.DataFrame:
        """
        Reads in the JSON data pointed at by the path used to initialize
        the Data_wrangler class.

        Return
        ------
        - fda_tables_unpacked (pd.DataFrame): an pd.DataFrame with 7 records,
            where each record is a pair containing an FDA table name and 
            its associated data
        """
        fda_tables_unpacked: pd.DataFrame = pd.read_json(self.path)
        return fda_tables_unpacked

    def unpack_data(self) -> None:
        """
        Unpacks nested data (pd.DataFrame) with two columns
        (tableName, data) into a dictionary.
        
        Each column of the nested data has 7 items:
        1. tableName (str): The names of the scraped tables
        2. data (list[dict[str, str]]): A list of the table's rows as
           a dict with key-value pairs as table column headers and row
           entries 

        The dictionary (dict[str, pd.DataFrame]) has 7 entries with
        table names and table data as key-value pairs 
        
        Time Complexity
        ---------------
        O(n)

        Assumptions
        -----------
            - The fda_tables_unpacked is a pandas data frame with the
               columns tableName and data 
        """
        # Reading in the unnested data
        fda_tables_unpacked: pd.DataFrame = self._read_file()
        
        # Initializing a dictionary to hold each of the scraped FDA
        # tables
        fda_tables: dict[str, pd.DataFrame] = dict()

        # Unpacking the unnested data
        for index, row in fda_tables_unpacked.iterrows():
            table_name: str = row['tableName']
            table_data: Any = row['data']

            try:
                new_df: pd.DataFrame = pd.json_normalize(table_data)
            except Exception as e:
                print(f'Could not normalize table {table_name}. Error: {e}')

                try:
                    new_df = pd.DataFrame(table_data)
                except Exception as e_simple:
                    print(f'Error: Could not create a data frame for table {table_name}.')
                    print(f'Skipping. Error: {e_simple}')
                    continue

            fda_tables[table_name] = new_df

        self.structure: list[str] = [
            'The scraped data is structured as a dictionary',
            '(dict[str, pd.DataFrame]) with 7 entries. Each',
            'entry key is a table name as a string and each',
            'value is the table data as a pd.DataFrame.'
        ]
        self.keys: list[str] = list(fda_tables.keys())
        self.tables: list[pd.DataFrame] = list(fda_tables.values())
        self.num_entries: int = len(self.keys)
        self.scraped_data: dict[str, pd.DataFrame] = fda_tables
        self.is_data_unpacked = 1

    def metadata(self) -> None:
        """
        Describes the structure of the scraped data object.

        Time Complexity
        ---------------
        O(1)
        """
        if not self.is_data_unpacked:
            print('Please unpack the data before trying to read the metadata.')
            return

        gen_info: list[str] = ['The attributes of the unnested data include:',
                               'structure', 'keys', 'tables', 'num_entires', 'scraped_data']
        print(*gen_info, sep = '\n', end = '\n\n')
        print(*self.structure, sep = '\n', end = '\n\n')
        print('Keys:', *self.keys, sep = '\n', end = '\n\n')
        print(f'Number of entries: {self.num_entries}', end = '\n\n')
        print('Here is the first table:')
        print('Columns:', *self.tables[0].columns.tolist(), sep = '\n', end = '\n\n')
        print(self.tables[0].head(), end = '\n\n')
