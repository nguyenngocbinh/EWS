import logging
import pandas as pd

def etl_db2_to_sqlserver_manually(src_cnxn, tgt_cnxn, src_table, tgt_table,
                                  src_cols=None, src_date_col='PROCESS_DATE',
                                  tgt_cols=None, tgt_date_col='PROCESS_DATE',
                                  tgt_data_types=None):
    """
    Perform ETL (Extract, Transform, Load) from a DB2 source table to a SQL Server target table.

    Parameters:
    - src_cnxn: DB2 database connection.
    - tgt_cnxn: SQL Server database connection.
    - src_table (str): Name of the source table in the DB2 database.
    - tgt_table (str): Name of the target table in the SQL Server database.
    - src_cols (list): List of column names to extract from the source table. If None, all columns will be selected.
    - src_date_col (str): Name of the date column in the source table. Default is 'PROCESS_DATE'.
    - tgt_cols (list): List of column names in the target table. If provided, source columns will be renamed accordingly.
    - tgt_date_col (str): Name of the date column in the target table. Default is 'PROCESS_DATE'.
    - tgt_data_types (list): List containing data types corresponding target table column names.
    """
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Query maximum PROCESS_DATE from target table
    max_tgt_date_query = f"SELECT MAX({tgt_date_col}) FROM {tgt_table}"
    max_tgt_date = pd.read_sql(max_tgt_date_query, tgt_cnxn).iloc[0, 0]

    # Extract data from source
    src_query = f"SELECT {', '.join(src_cols) if src_cols else '*'} FROM {src_table} WHERE {src_date_col} > '{max_tgt_date}'"
    src_data = pd.read_sql(src_query, src_cnxn)

    if src_data.empty:
        logging.warning("Source data is empty. No rows extracted.")
    else:
        logging.info("ETL from %s to %s started.", src_table, tgt_table)

    logging.info("Maximum target data date: %s", max_tgt_date)

    # Check if lengths of src_cols and tgt_cols are different
    if src_cols and tgt_cols and len(src_cols) != len(tgt_cols):
        logging.warning("Length of src_cols is different from length of tgt_cols. Columns may not align correctly.")

    # Rename columns to correspond to target table
    if tgt_cols:
        src_data.rename(columns=dict(zip(src_cols, tgt_cols)), inplace=True)

    # Check if lengths of src_cols and tgt_cols are different
    if tgt_data_types and tgt_cols and len(tgt_data_types) != len(tgt_cols):
        logging.warning("Length of tgt_data_types is different from length of tgt_cols. Columns may not align correctly.")
        
    # Format source data types to align with target data types
    if tgt_data_types:
        src_data = format_data_types(src_data, zip(tgt_cols, tgt_data_types))

    # Load data into target
    src_data.to_sql(tgt_table, tgt_cnxn, if_exists='append', index=False, chunksize=100000)

def format_data_types(src_data, tgt_data_types):
    """
    Format source data types to align with target data types.

    Parameters:
    - src_data (DataFrame): Source data to be formatted.
    - tgt_data_types (dict): Dictionary containing target table column names and their corresponding data types.

    Returns:
    - DataFrame: Formatted source data.
    """
    for column, data_type in tgt_data_types.items():
        src_data[column] = src_data[column].astype(data_type)
    return src_data
