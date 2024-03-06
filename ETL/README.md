# ETL Data from DB2 to SQL Server

This project facilitates Extract, Transform, and Load (ETL) processes for various data sources and destinations. It includes scripts and configurations to efficiently manage data movement.

### Project Structure

```
ETL
├─ config
│  ├─ db2_config.yaml
│  ├─ sql_server_config.yaml
│  └─ tables
│     ├─ exchange_rate.yaml
│     ├─ ifrs9_collateral.yaml
│     ├─ ifrs9_cst_cc.yaml
│     ├─ ifrs9_cst_cc_txn.yaml
│     ├─ ifrs9_cst_ln.yaml
│     ├─ ifrs9_ctr_cl.yaml
│     ├─ ifrs9_ctr_lc.yaml
│     ├─ ifrs9_ctr_od.yaml
│     ├─ ifrs9_cust_info.yaml
│     ├─ ifrs9_dep_amt.yaml
│     ├─ ifrs9_dep_amt_txn.yaml
│     └─ ifrs9_limit.yaml
├─ README.md
├─ requirements.txt
├─ scripts
│  ├─ add_more_jobs.sql
│  ├─ create job.sql
│  └─ etl_exchange_rate.py
├─ src
│  ├─ db.py
│  ├─ etl.py
│  ├─ etl_manualy.py
│  └─ utils.py
├─ tests
│  ├─ connect_to_db.py
│  ├─ etl.ipynb
│  ├─ etl_v1.ipynb
│  └─ test_engine_creation.py
└─ __init__.py
```

### Directory Structure

- **config**: Contains configuration files for database connections and table definitions.
- **scripts**: Includes SQL scripts for managing jobs and Python scripts for ETL processes.
- **src**: Houses Python source code for database connection management and ETL operations.
- **tests**: Contains scripts and notebooks for testing database connections and ETL functionality.

### Usage

1. **Configurations**: Modify configuration files in the `config` directory according to your database settings.
2. **Requirements**: Install project dependencies using `pip install -r requirements.txt`.
3. **Scripts**: Execute Python scripts in the `scripts` directory for ETL processes.
4. **SQL Scripts**: Run SQL scripts in the `scripts` directory to manage jobs and database tasks.
5. **Testing**: Use scripts and notebooks in the `tests` directory to test database connections and ETL functionality.

### Example how to run

1. **Configure Database**:

    Edit `db2_config.yaml` and `sql_server_config.yaml` to set up the database connections.

2. **Configure general information**:

    Edit the respective YAML files in the `tables` directory to specify the source and target tables along with their columns and data types.

    Example (`exchange_rate.yaml`):

    ```yaml
    source_table: 'CSO.FCC_CYTB_RATES_HISTORY_OFFICIAL'
    target_table: 'EXCHANGE_RATE'
    source_columns:
      - 'column1'
      - 'column2'
    source_date_column: 'RATE_DATE'
    target_columns:
      - 'col1'
      - 'col2'
    target_date_column: 'TARGET_RATE_DATE'
    target_data_types:
      column1: 'VARCHAR(50)'
      column2: 'INTEGER'
    tgt_primary_key_columns:
      - 'col1'
      - 'col2'
    ```

3. **Run Script**:

    Execute the Python script to start the ETL process.

    ```
    python scripts\etl_exchange_rate.py
    ```

### Contributors

- Nguyễn Ngọc Bình
