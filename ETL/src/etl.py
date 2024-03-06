
import pandas as pd
import logging
from src.utils import format_data_types, load_config

class ETL:
    def __init__(self, src_cnxn, tgt_cnxn):
        """
        Initialize the ETL (Extract, Transform, Load) class.

        Parameters:
        - src_cnxn (object): Connection object for the DB2 database.
        - tgt_cnxn (object): Connection object for the SQL Server database.
        """
        self.src_cnxn = src_cnxn
        self.tgt_cnxn = tgt_cnxn        

    def etl_db2_to_sqlserver(self, etl_config):
        """
        Perform ETL (Extract, Transform, Load) from a DB2 source table to a SQL Server target table.

        Parameters:
        - etl_config (dict): Dictionary containing ETL configuration parameters.

        Example etl_config:
        etl_config = {
            'source_table': 'CSO.FCC_CYTB_RATES_HISTORY_OFFICIAL',
            'target_table': 'EXCHANGE_RATE',
            'source_columns': None,
            'source_date_column': 'RATE_DATE',
            'target_columns': None,
            'target_date_column': 'TARGET_RATE_DATE',
            'target_data_types': None
        }
        """
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        
        # Load etl_config from YAML file if a path is provided
        if isinstance(etl_config, str):   
            etl_config = load_config(etl_config)                
                
        src_table = etl_config.get('source_table')
        tgt_table = etl_config.get('target_table')
        src_cols = etl_config.get('source_columns')
        src_date_col = etl_config.get('source_date_column')
        tgt_cols = etl_config.get('target_columns')
        tgt_date_col = etl_config.get('target_date_column')
        tgt_data_types = etl_config.get('target_data_types')
        
        # Check if lengths of src_cols and tgt_cols are different
        if src_cols and tgt_cols and len(src_cols) != len(tgt_cols):
            logging.warning("Length of src_cols is different from length of tgt_cols. Columns may not align correctly.")
        
        # Check if lengths of src_cols and tgt_cols are different
        if tgt_data_types and tgt_cols and len(tgt_data_types) != len(tgt_cols):
            logging.warning("Length of tgt_data_types is different from length of tgt_cols. Columns may not align correctly.")

        # Query maximum PROCESS_DATE from target table
        max_tgt_date_query = f"SELECT MAX({tgt_date_col}) FROM {tgt_table}"
        max_tgt_date = pd.read_sql(max_tgt_date_query, self.tgt_cnxn).iloc[0, 0]
                
        # Extract data from source
        src_query = f"SELECT {', '.join(src_cols) if src_cols else '*'} FROM {src_table} WHERE {src_date_col} > '{max_tgt_date}'"
        src_data = pd.read_sql(src_query, self.src_cnxn)

        if src_data.empty:
            logging.warning("Source data is empty. No rows extracted.")
        else:
            logging.info("ETL from %s to %s started.", src_table, tgt_table)
            
        logging.info("Maximum target data date: %s", max_tgt_date)

        # Rename columns to correspond to target table
        if tgt_cols:
            src_data = src_data.rename(columns=dict(zip(src_cols, tgt_cols)))
        
        # Format source data types to align with target data types
        if tgt_data_types:
            src_data = format_data_types(src_data, dict(zip(src_cols, tgt_data_types)))

        # Load data into target
        src_data.to_sql(tgt_table, self.tgt_cnxn, if_exists='append', index=False, chunksize=100000)
        
    



        