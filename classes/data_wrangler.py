"""
Data_Wrangler reads in a JSON file and restructures its data into a
pandas dataframe. The data_wrangler class lacks generalizability 
at the moment, but regex and time can fix that.
"""

# Loading dependencies

# pandas enables data wrangling
import pandas as pd 

# Any serves as a type hint for objects with a complex structure
# List and Dict are self-explanatory
from typing import Any, List, Dict

# sqlite3 enables caching
import sqlite3
from sqlite3 import Connection, Cursor

# re is for regex
import re

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
    - path() -> str
    - _is_data_unpacked() -> int
    - _structure() -> str
    - _keys() -> List[str]
    - _tables() -> List[pd.DataFrame]
    - _num_entries() -> int
    - _scraped_data() -> dict[str, pd.DataFrame]
    - _read_file() -> pd.DataFrame
    - unpack_data() -> Dict[str, pd.DataFrame]
    - metadata() -> str
    - cache_data() -> str
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
    def structure(self) -> List[str]:
        """Description of the structure of the unpacked JSON data."""
        return self._structure
    
    @structure.setter
    def structure(self, structure: List[str]) -> None:
        if isinstance(structure, List):            
            for item in structure:
                if not isinstance(item, str):
                    raise TypeError('Each item in structure must be a str.')  
            self._structure = structure
        else:
            raise TypeError('The structure param must be a List.')
        
    @property
    def keys(self) -> List[str]:
        """Keys of the dictionary holding the unpacked JSON data."""
        return self._keys
    
    @keys.setter
    def keys(self, keys: List[str]) -> None:
        if isinstance(keys, List):            
            for item in keys:
                if not isinstance(item, str):
                    raise TypeError('Each item in keys must be a str.')  
            self._keys = keys
        else:
            raise TypeError('The keys param must be a List.')
        
    @property
    def tables(self) -> List[pd.DataFrame]:
        """Values (tables) of the dictionary holding the unpacked JSON data."""
        return self._tables
    
    @tables.setter
    def tables(self, tables: List[pd.DataFrame]) -> None:
        if isinstance(tables, List):            
            for item in tables:
                if not isinstance(item, pd.DataFrame):
                    raise TypeError('Each item in tables must be a pd.DataFrame.')  
            self._tables = tables
        else:
            raise TypeError('The tables param must be a List.')
        
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
        2. data (List[dict[str, str]]): A List of the table's rows as
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
        try:
            # Reading in the unnested data
            fda_tables_unpacked: pd.DataFrame = self._read_file()

            # Validating the existence of the required columns
            required_cols: List[str] = ['tableName', 'data']
            if not all(
                col in fda_tables.unpacked.columns.tolist() for col in required_cols
            ):
                cols: str = ', '.join(required_cols).lstrip(', ')
                raise ValueError(f"Input JSON is missing the required columns: {cols}")
        
            # Initializing a dictionary to hold each of the scraped FDA tables
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

            # Checking if all data was unpacked
            if not fda_tables:
                print('Warning: Not all tables were successfully unpacked.')
                return

            self.structure: List[str] = [
                'The scraped data is structured as a dictionary',
                '(dict[str, pd.DataFrame]) with 7 entries. Each',
                'entry key is a table name as a string and each',
                'value is the table data as a pd.DataFrame.'
            ]
            self.keys: List[str] = list(fda_tables.keys())
            self.tables: List[pd.DataFrame] = list(fda_tables.values())
            self.num_entries: int = len(self.keys)
            self.scraped_data: dict[str, pd.DataFrame] = fda_tables
            self.is_data_unpacked = 1

        except FileNotFoundError:
            # Catches missing file errors
            print(f'Error: The file at {self.path} was not found.')
            self.is_data_unpacked = 0
        
        except ValueError as e:
            # Catches JSON or missing column errors
            print(f'Data Error: {e}')

        except Exception as e:
            # Catches unexpected errors
            print(f'An unexpected error occurred during unpacking: {e}')
            self.is_data_unpacked = 0

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

        try:
            gen_info: List[str] = ['The attributes of the unnested data include:',
                                'structure', 'keys', 'tables', 'num_entires', 'scraped_data']
            print(*gen_info, sep = '\n', end = '\n\n')
            print(*self.structure, sep = '\n', end = '\n\n')
            print('Keys:', *self.keys, sep = '\n', end = '\n\n')
            print(f'Number of entries: {self.num_entries}', end = '\n\n')
            print('Here is the first table:')
            print('Columns:', *self.tables[0].columns.tolist(), sep = '\n', end = '\n\n')
            print(self.tables[0].head(), end = '\n\n')

        except AttributeError as e:
            print(f'Error: Missing data attributes. Data might be corrupted. {e}')
        
        except Exception as e:
            print(f'An unexpected error occurred while generating metadata: {e}')

    def _init_db(self) -> bool:
        """
        Initializes the SQL database for caching the data.

        Return
        ------
        - flag (int): flag indicating the outcome of initializing the SQL database,
            where 1 indicates success and 0 indicates failure  
        """

        if not self.is_data_unpacked:
            print('Please unpack the data before trying to cache it.')
            return False

        # Establishing a connection to the database
        conn: Connection = sqlite3.connect('Food_Recalls.db')

        # Creating a cursor
        cur: Cursor = conn.cursor()

        # Deleting any pre-existing tables under the same name
        cur.execute("DROP TABLE IF EXISTS Food_Recalls")

        # Adding the table name to each table
        for idx in range( len(self.tables) ):
            self.tables[idx].insert(0, 'TableName', self.keys[idx])
        
        # Getting the column names found in each table
        colnames_list: List[str] = self.tables[0].columns.tolist()

        # Wrappign column names in double quotes for SQLite3
        colnames_list = [f'"{name.replace("\"","")}"' for name in colnames_list]
        
        # Joing the column names to facilitate building queries
        colnames: str = ', '.join(colnames_list)
        colnames = colnames.lstrip(', ')

        # Creating the DB table
        cur.execute(f"CREATE TABLE IF NOT EXISTS Food_Recalls({colnames})")

        # Committing the CREATE TABLE query
        conn.commit()

        # Getting the flag to indicate if the CREATE TABLE query 
        # executed successfully
        res: Cursor = cur.execute('SELECT name FROM sqlite_master')
        table_name: str = res.fetchone()[0]

        # Closing the connection to the database
        conn.close()

        return (table_name is not None)

    def _insert_data(self) -> None:
        """Inserts scraped data into a SQL database."""
        # Establishing a connection to the database
        conn: Connection = sqlite3.connect('Food_Recalls.db')

        # Creating a cursor
        cur: Cursor = conn.cursor()

        # Getting the column names found in each table
        colnames_list: List[str] = self.tables[0].columns.tolist()

        # Wrappign column names in double quotes for SQLite3
        colnames_list = [f'"{name.replace("\"","")}"' for name in colnames_list]
        
        # Joing the column names to facilitate building queries
        colnames: str = ', '.join(colnames_list)
        colnames = colnames.lstrip(', ')

        # Defining the INSERT query for the table foodborne_outbreaks
        insert_statement: str = f"INSERT INTO Food_Recalls ({colnames}) VALUES"

        # Looping through the scraped FDA tables and inserting their data into
        # the SQL Database (Food_Recalls)
        for table in self.tables:
            # Getting the number of rows in the current table
            nrow: int = table.shape[0]

            # Looping through the rows of the current table
            for r in range(nrow):
                # Getting the current row's data
                row_list: List[Any] = table.iloc[r].tolist()

                # Formatting the current row's data so it is compatible with SQLite3
                row_list = [f'"{name.replace("\"","")}"' for name in row_list]
                row: str = ', '.join(row_list)
                row = '(' + row.lstrip(', ') + ')'

                # Creating the full INSERT query 
                query: str = insert_statement + ' ' + row

                # Executing the query
                cur.execute(query)
        
        # Committing all INSERT queries
        conn.commit()

        # Closing the connection
        conn.close()

    def cache_data(self) -> None:
        """
        Uses a SQL database to cache scraped data.
        """
        try:
            # Initializing the SQL Database
            success: bool = self._init_db()
            if not success:
                print('Error: Failed database initialization.')
                return
            
            # Inserting the data
            self._insert_data()
            print('Data caching was successful.')

        except sqlite3.Error as e:
            # Catching SQL database errors
            print(f'SQL Error occurred: {e}')

        except Exception as e:
            # Catching other errors
            print(f'An unexpected error occurred: {e}')


    






