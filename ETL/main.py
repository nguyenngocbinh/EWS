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
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_collateral.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_cst_cc.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_cst_cc_txn.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_cst_ln.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_ctr_cl.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_ctr_lc.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_ctr_od.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_ctr_cc.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_cust_info.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_dep_amt.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_dep_amt_txn.yaml')
    etl_obj.etl_db2_to_sqlserver('config/tables/ifrs9_limit.yaml')

if __name__ == "__main__":
    main()