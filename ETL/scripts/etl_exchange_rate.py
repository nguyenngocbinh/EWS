import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import create_db2_engine, create_sql_server_engine
from src.etl import ETL

def main():
    # Create DB2 engine
    db2_engine = create_db2_engine()
    print("DB2 engine created successfully.")
        
    # Create SQL Server engine
    sql_server_engine = create_sql_server_engine()
    print("SQL Server engine created successfully.")

    etl_obj = ETL(db2_engine, sql_server_engine)
    etl_obj.etl_db2_to_sqlserver('config/tables/exchange_rate.yaml')

if __name__ == "__main__":
    main()
