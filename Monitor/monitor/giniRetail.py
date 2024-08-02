import logging
from .performance import PortfolioPerformance
from .psi import calculate_psi, calculate_psi_for_features
from .db import create_sql_server_engine
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import types

def giniRetail():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Server configuration
    server_config_bscore = {
        'server': 'VM-DC-JUMPSRV77\\IFRS9',
        'database': 'EWS'
    }

    logger.info("Creating SQL Server engine.")
    conn_bscore = create_sql_server_engine(server_config_bscore, use_windows_auth=True)

    # Calculate the date 13 months ago from today
    today = datetime.today()
    twelve_months_ago = today - relativedelta(months=13)

    # Get the last day of the month 13 months ago
    last_day_of_month = twelve_months_ago.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
    end_date = last_day_of_month.strftime('%Y-%m-%d')

    logger.info(f"End date calculated as {end_date}.")

    # Fetch the start date from the database
    logger.info("Fetching the start date from the database.")
    _start_date_query = '''
    SELECT MAX(process_date) FROM GINI_Bscore where portfolio in ('autoloan','mortgage', 'others', 'secured', 'unsecured')
    '''
    start_date = pd.read_sql(_start_date_query, conn_bscore).iloc[0, 0]
    start_date = start_date.strftime('%Y-%m-%d')
    logger.info(f"Start date fetched as {start_date}.")

    # Query to fetch retail data
    retail_query = f''' 
    WITH a AS (
        SELECT *, 
        CASE 
            WHEN portfolio = 'autoloan' THEN 'Autoloan'
            WHEN portfolio = 'mortgage' THEN 'Mortgage'
            WHEN portfolio = 'others' THEN 'Others'
            WHEN portfolio = 'secured' THEN 'Consumer lending - secured'
            WHEN portfolio = 'unsecured' THEN 'Consumer lending - unsecured'
            ELSE 'Unknown'
        END AS SEGMENT_ODR_LV1
        FROM EWS..ews_khcn_portfolio_score_store
        WHERE process_date > '{start_date}' AND process_date <= '{end_date}'
    )
    SELECT 
        a.process_date,
        a.customer_id,
        b.customer_type,
        a.portfolio,
        a.grade,
        a.grade_num,
        a.score,
        b.default_flag		
    FROM a
    INNER JOIN IFRS9_PROVISIONAL..CUSTOMER_ODR_LRA b 
        ON a.customer_id = b.customer_id
        AND a.process_date = b.process_date
        AND a.SEGMENT_ODR_LV1 = b.SEGMENT_ODR_LV1
    WHERE a.model = 'B-score';
    '''

    logger.info("Fetching retail data from the database.")
    df_retail = pd.read_sql_query(retail_query, conn_bscore)
    logger.info(f"Retail data fetched. Number of records: {len(df_retail)}")

    # Create an instance of the class
    logger.info("Creating PortfolioPerformance instance.")
    performance_calculator = PortfolioPerformance(df_retail)

    # Calculate performance for the date range
    logger.info("Calculating performance metrics.")
    result = performance_calculator.calculate_gini_ks(start_date, end_date)

    # Save the result to the database
    table_name = 'GINI_Bscore'
    logger.info(f"Saving results to table {table_name}.")
    result.to_sql(name=table_name, con=conn_bscore, if_exists='append', index=False, dtype={
        'process_date': types.Date,
        'portfolio': types.String,
        'gini': types.Float,
        'ks': types.Float,
        'ks_pvalue': types.Float
    })

    logger.info("Results saved to the database.")