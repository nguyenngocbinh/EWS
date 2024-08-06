import os
import sys
import logging

# Add parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import create_db2_engine, create_sql_server_engine, DatabaseHandler
from src.etl import ETL

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # # Create custom database handler for logging
    # db_log_config = {
    #     'user': 'my_user',
    #     'password': 'my_password',
    #     'server': 'localhost',
    #     'database': 'my_db'
    # }

    # db_handler = DatabaseHandler(sql_server_config=db_log_config, use_windows_auth=False)
    db_handler = DatabaseHandler()
    db_handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(db_handler)

    try:
        # Create SQL Server engine
        sql_server_engine = create_sql_server_engine()
        logging.info("SQL Server engine created successfully.")
    except Exception as e:
        logging.error(f"Failed to create SQL Server engine: {e}")
        return

    try:
        # DB2 engine creation
        db2_engine = create_db2_engine()  
        logging.info("DB2 engine created successfully.")
    except Exception as e:
        logging.error(f"Failed to create DB2 engine: {e}")
        return

    try:
        etl_obj = ETL(db2_engine, sql_server_engine)
        tables = [
            'exchange_rate',
            'ifrs9_collateral',
            'ifrs9_cst_cc',
            'ifrs9_cst_cc_txn',
            'ifrs9_cst_ln',
            'ifrs9_ctr_cl',
            'ifrs9_ctr_lc',
            'ifrs9_ctr_od',
            'ifrs9_ctr_cc',
            'ifrs9_cust_info',
            'ifrs9_dep_amt',
            'ifrs9_dep_amt_txn',
            'ifrs9_limit'
        ]
        
        for table in tables:
            etl_obj.etl_db2_to_sqlserver(f'config/tables/{table}.yaml')
            logging.info(f"ETL process completed for table: {table}")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")

if __name__ == "__main__":
    main()
