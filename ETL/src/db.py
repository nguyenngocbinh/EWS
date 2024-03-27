import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote
from src.utils import load_config
import os

def create_db2_engine(db2_config=None):
    """
    Create a DB2 database engine.

    Parameters:
    - db2_config (dict or str, optional): Dictionary containing DB2 database connection parameters or path to the YAML configuration file.
      If None, default configuration from 'config/db2_config.yaml' will be used.

    Returns:
    - engine: DB2 database engine.
    
    Example:
    db2_config = {
        'user': 'my_user',
        'password': 'my_password',
        'host': 'localhost',
        'port': '50000',
        'database': 'my_db'
    }
    engine = create_db2_engine(db2_config)
    """
    # Load etl_config from YAML file if a path is provided
    if db2_config is None:        
        default_config_path = 'config/db2_config.yaml'
        db2_config = load_config(default_config_path)
    elif isinstance(db2_config, str):   
        db2_config = load_config(db2_config) 

    user = db2_config['user']
    password = quote(db2_config['password'])
    host = db2_config['host']
    port = db2_config['port']
    database = db2_config['database']
    db2_url = f"ibm_db_sa://{user}:{password}@{host}:{port}/{database}"
    return create_engine(db2_url)

def create_sql_server_engine(sql_server_config=None):
    """
    Create a SQL Server database engine.

    Parameters:
    - sql_server_config (dict or str, optional): Dictionary containing SQL Server database connection parameters or path to the YAML configuration file.
      If None, default configuration from 'config/sql_server_config.yaml' will be used.

    Returns:
    - engine: SQL Server database engine.
    
    Example:
    sql_server_config = {
        'user': 'my_user',
        'password': 'my_password',
        'server': 'localhost',
        'database': 'my_db'
    }
    engine = create_sql_server_engine(sql_server_config)
    """    
    # Load etl_config from YAML file if a path is provided
    if sql_server_config is None:
        default_config_path = os.path.join('config', 'sql_server_config.yaml')
        sql_server_config = load_config(default_config_path)
    elif isinstance(sql_server_config, str):   
        sql_server_config = load_config(sql_server_config) 

    sqlserver_driver = '{ODBC Driver 17 for SQL Server}'
    server = sql_server_config['server']
    database = sql_server_config['database']
    user = sql_server_config['user']
    password = sql_server_config['password']
    sqlserver_cnxn_str = f"DRIVER={sqlserver_driver};SERVER={server};DATABASE={database};UID={user};PWD={password}"
    odbc_connect_str = quote(sqlserver_cnxn_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_connect_str}", use_setinputsizes=False)


class DatabaseConnector:
    def __init__(self, db2_user, db2_password, db2_host, db2_port, db2_database,
                 sqlserver_server, sqlserver_database, sqlserver_user, sqlserver_pw):
        """
        Initialize the DatabaseConnector class.
        
        Parameters:
        - db2_user (str): Username for connecting to the DB2 database.
        - db2_password (str): Password for connecting to the DB2 database.
        - db2_host (str): Hostname or IP address of the DB2 server.
        - db2_port (int): Port number of the DB2 server.
        - db2_database (str): Name of the DB2 database.
        - sqlserver_server (str): Server name for connecting to the SQL Server database.
        - sqlserver_database (str): Name of the SQL Server database.
        - sqlserver_user (str): Username for connecting to the SQL Server database.
        - sqlserver_pw (str): Password for connecting to the SQL Server database.

        # Example usage:
        db_connector = DatabaseConnector('tpbrm1', 'Tpbrm1@12345', '10.1.50.25', '50000', 'bludb',
                                 'VM-DC-JUMPSRV77\IFRS9', 'EWS', 'rdm_admin', '2024#tpb')

        """
        self.db2_user = db2_user
        self.db2_password = db2_password
        self.db2_host = db2_host
        self.db2_port = db2_port
        self.db2_database = db2_database
        self.sqlserver_server = sqlserver_server
        self.sqlserver_database = sqlserver_database
        self.sqlserver_user = sqlserver_user
        self.sqlserver_pw = sqlserver_pw
        
        self.db2_engine = self._create_db2_engine()
        self.sqlserver_engine = self._create_sqlserver_engine()

    def _create_db2_engine(self):
        encoded_password = quote(self.db2_password)
        db2_url = f'ibm_db_sa://{self.db2_user}:{encoded_password}@{self.db2_host}:{self.db2_port}/{self.db2_database}'
        return create_engine(db2_url)

    def _create_sqlserver_engine(self):
        sqlserver_driver = '{ODBC Driver 17 for SQL Server}'
        sqlserver_cnxn_str = f"DRIVER={sqlserver_driver};SERVER={self.sqlserver_server};DATABASE={self.sqlserver_database};UID={self.sqlserver_user};PWD={self.sqlserver_pw}"
        odbc_connect_str = quote(sqlserver_cnxn_str)
        return create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_connect_str}")

    def execute_query_db2(self, query):
        with self.db2_engine.connect() as connection:
            result = connection.execute(query)
            return result

    def execute_query_sqlserver(self, query):
        with self.sqlserver_engine.connect() as connection:
            result = connection.execute(query)
            return result

class TableModifier:
    """
    A class for modifying tables in a SQL database.

    Attributes:
    engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine connected to the database.
    """

    def __init__(self, engine):
        """
        Initializes the TableModifier class.

        Args:
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine connected to the database.
        """
        self.engine = engine
    
    def get_original_data_types(self, table, columns):
        """
        Retrieves the original data types of specified columns in a table.

        Args:
        table (str): The name of the table.
        columns (list): A list of column names.

        Returns:
        dict: A dictionary mapping column names to their original data types.
        """
        original_data_types = {}
        with self.engine.connect() as connection:
            for column in columns:
                _query = f"SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND COLUMN_NAME = '{column}'"
                result = connection.execute(_query)
                data_type = result.fetchone()
                if data_type:
                    if data_type[0] == 'date':
                        original_data_types[column] = 'date'
                    else:
                        original_data_types[column] = f"{data_type[0]}({data_type[1]})" if data_type[1] else data_type[0]
                else:
                    print(f"Column '{column}' not found in table '{table}'")
                    return
        return original_data_types

    def alter_columns(self, table, columns, original_data_types):
        """
        Alters specified columns in a table to not allow null values.

        Args:
        table (str): The name of the table.
        columns (list): A list of column names.
        original_data_types (dict): A dictionary mapping column names to their original data types.
        """
        alter_sql_commands = []
        for column in columns:
            alter_sql_commands.append(f"ALTER TABLE {table} ALTER COLUMN {column} {original_data_types[column]} NOT NULL")
        with self.engine.connect() as connection:
            for command in alter_sql_commands:
                connection.execute(command)

    def create_index(self, table, columns):
        """
        Creates an index on specified columns in a table.

        Args:
        table (str): The name of the table.
        columns (list): A list of column names.
        """
        index_name = f"idx_{table.split('.')[-1]}"
        
        # Generate SQL commands to create index
        index_sql_command = f"CREATE INDEX idx_{table.split('.')[-1]} ON {table} ({', '.join(columns)})"
        
        # Generate SQL command to drop existing index
        drop_index_sql_command = f"DROP INDEX IF EXISTS idx_{table.split('.')[-1]} ON {table}"

        with self.engine.connect() as connection:
            # Drop existing primary key constraint if it exists
            connection.execute(drop_index_sql_command)
            # Create new index 
            connection.execute(index_sql_command)

    def create_primary_key(self, table, columns):
        """
        Creates a primary key constraint on specified columns in a table.

        Args:
        table (str): The name of the table.
        columns (list): A list of column names.
        """
        pk_name = f"PK_{table.split('.')[-1]}"
        # Generate SQL command to drop existing primary key constraint
        drop_primary_key_sql_command = f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS PK_{table.split('.')[-1]}"
        
        # Generate SQL commands to create primary key
        primary_key_sql_command = f"ALTER TABLE {table} ADD CONSTRAINT PK_{table.split('.')[-1]} PRIMARY KEY ({', '.join(columns)})"

        with self.engine.connect() as connection:
            # Drop existing primary key constraint if it exists
            connection.execute(drop_primary_key_sql_command)
            # Create new primary key
            connection.execute(primary_key_sql_command)

    def alter_columns_and_create_index_primary(self, table, columns):
        """
        Alters specified columns to not allow null values, creates an index, and sets a primary key constraint.

        Args:
        table (str): The name of the table.
        columns (list): A list of column names.
        """
        original_data_types = self.get_original_data_types(table, columns)
        self.alter_columns(table, columns, original_data_types)
        self.create_index(table, columns)
        self.create_primary_key(table, columns)