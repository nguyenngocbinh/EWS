from src.scoring import predict
from src.db import create_sql_server_engine
from src.fix import fix_ews_khcn
from datetime import datetime, timedelta
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_end_of_last_month():
    """
    Calculate the end of the last month.

    Returns:
    - str: End of last month in 'YYYY-MM-DD' format.
    """
    return (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')

def predictRetail():
    try:
        # Server configuration
        server_config_bscore = {
            'server': 'VM-DC-JUMPSRV77\\IFRS9',
            'database': 'EWS'
        }

        # Create SQL Server engine
        logger.info("Creating SQL Server engine")
        conn = create_sql_server_engine(server_config_bscore, use_windows_auth=True)

        # Calculate the end of last month
        end_of_last_month = get_end_of_last_month()

        # SQL query to select the features
        query = f"""
        SELECT 
            process_date,
            deal_sub_type as deal_subtype,
            customer_id,
            branch_code,
            bucket,
            model,
            current_default,
            mob_portfolio,
            max_dpd_portfolio,
            avg_bal_l3m,
            avg_bal_onbs_l6m,
            avg_cr_txn_amt_vs_os_l6m,
            avg_total_num_txn_l3m,
            avg_total_num_txn_l6m,
            dpd_l3m,
            dpd_l6m,
            max_dpd_loan,
            max_os_onbs_l6m,
            min_day_to_matdt,
            num_cr_product_l3m,
            os_amt_long_curr,
            os_revl,
            os_revl_l3m,
            remain_term,
            remain_term_autoloan,
            total_num_txn_l6m,
            vol_os_l6m,
            avg_bal_l6m,
            avg_total_num_txn,
            avg_txn_amt_os_onbs_l3m,
            bal_l6m,
            ca_bal,
            ca_bal_l3m,
            ca_bal_os_onbs_avg,
            ca_bal_os_onbs_l3m_avg,
            ca_td_bal_legacy,
            max_net_flow_txn_amt_l3m,
            min_ca_bal_l6m,
            total_bal_avg,
            total_num_txn,
            total_num_txn_l3m
        FROM [EWS].[dbo].[ews_portfolio_shortlist_store]
        WHERE process_date = '{end_of_last_month}'
        """

        # Fetch data into a DataFrame
        logger.info("Fetching data into DataFrame")
        df = pd.read_sql(query, conn)

        # SQL query to fetch master scale data
        logger.info("Fetching master scale data into DataFrame")
        master_scale_query = f""" 
            SELECT 
                portfolio,
                deal_subtype,
                grade,
                ttc_pd,
                score_lower,
                score_upper,
                grade_num,
                eff_date,
                exp_date
            FROM [EWS].[dbo].[master_scale_portfolio]
            WHERE '{end_of_last_month}' BETWEEN eff_date AND exp_date
        """

        # Fetch master scale into a DataFrame
        master_scale = pd.read_sql(master_scale_query, conn)

        # Make prediction
        logger.info("Making predictions")
        pred = predict(df)

        # Convert deal_subtype to uppercase in both DataFrames
        logger.info("Merging pred and master_scale DataFrames on deal_subtype")
        pred['deal_subtype'] = pred['deal_subtype'].str.upper()
        master_scale['deal_subtype'] = master_scale['deal_subtype'].str.upper()

        # Merge the DataFrames on deal_subtype
        merged_df = pd.merge(pred, master_scale, on='deal_subtype')

        # Filter the merged DataFrame to include only the rows where the score falls between score_lower and score_upper
        filtered_df = merged_df[(merged_df['score'] >= merged_df['score_lower']) & (merged_df['score'] <= merged_df['score_upper'])]

        # Define the mapping of deal_sub_type to portfolio
        deal_subtype_to_portfolio = {
            'IBA_OD': 'overdraft',
            'LN_AUTO': 'autoloan',
            'LN_CONS_SEC': 'secured',
            'LN_CONS_UNSEC': 'unsecured',
            'LN_OTHERS': 'others'
        }

        # Create the portfolio column based on the mapping
        filtered_df.loc[:, 'portfolio'] = filtered_df['deal_subtype'].map(deal_subtype_to_portfolio)

        # Rename deal_subtype column back to deal_sub_type
        filtered_df = filtered_df.rename(columns={'deal_subtype': 'deal_sub_type'})

        # Apply fix_ews_khcn function
        filtered_df = fix_ews_khcn(filtered_df, master_scale)

        # Select specific columns from the filtered DataFrame
        logger.info("Selecting specific columns from filtered DataFrame")
        selected_columns = [
            'branch_code', 'portfolio', 'process_date', 'customer_id', 'score', 'grade',
            'ttc_pd', 'dpd_l6m', 'dpd_l3m', 'min_ca_bal_l6m', 'avg_total_num_txn_l6m',
            'avg_total_num_txn', 'avg_total_num_txn_l3m', 'avg_cr_txn_amt_vs_os_l6m',
            'avg_txn_amt_os_onbs_l3m', 'vol_os_l6m', 'remain_term_autoloan', 'max_os_onbs_l6m',
            'avg_bal_onbs_l6m', 'remain_term', 'total_bal_avg', 'ca_bal_os_onbs_avg',
            'total_num_txn_l6m', 'avg_bal_l3m', 'avg_bal_l6m', 'ca_bal_os_onbs_l3m_avg',
            'bal_l6m', 'os_amt_long_curr', 'total_num_txn_l3m', 'max_net_flow_txn_amt_l3m',
            'ca_bal_l3m', 'max_dpd_loan', 'total_num_txn', 'os_revl_l3m', 'os_revl', 'ca_bal',
            'min_day_to_matdt', 'num_cr_product_l3m', 'ca_td_bal_legacy', 'grade_num', 'bucket',
            'model', 'mob_portfolio', 'max_dpd_portfolio', 'deal_sub_type', 'current_default'
        ]

        # Ensure the selected columns are present in the filtered DataFrame
        filtered_df = filtered_df[selected_columns]

        # Output the filtered DataFrame to a SQL table
        logger.info("Outputting filtered DataFrame to SQL table 'tmp'")
        filtered_df.to_sql('tmp', con=conn, index=False, if_exists='replace')

        logger.info("Process completed successfully")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    predictRetail()
