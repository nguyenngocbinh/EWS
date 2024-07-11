import lightgbm as lgb
import pandas as pd
import numpy as np
import logging
import os

# Define the path to the model file
model_file_path = os.path.join(os.path.dirname(__file__), 'bscoreLoanLGBM.txt')

# Load the LightGBM model
try:
    bscoreLoanLGBM = lgb.Booster(model_file=model_file_path)
except Exception as e:
    logging.error(f"Error loading the model file: {e}")
    raise
    

# Define features
features = [
    "avg_bal_l3m", "avg_bal_onbs_l6m", "avg_cr_txn_amt_vs_os_l6m", "avg_total_num_txn_l3m",
    "avg_total_num_txn_l6m", "dpd_l3m", "dpd_l6m", "max_dpd_loan", "max_os_onbs_l6m",
    "min_day_to_matdt", "num_cr_product_l3m", "os_amt_long_curr", "os_revl", "os_revl_l3m",
    "remain_term", "remain_term_autoloan", "total_num_txn_l6m", "vol_os_l6m", "avg_bal_l6m",
    "avg_total_num_txn", "avg_txn_amt_os_onbs_l3m", "bal_l6m", "ca_bal", "ca_bal_l3m",
    "ca_bal_os_onbs_avg", "ca_bal_os_onbs_l3m_avg", "ca_td_bal_legacy", "max_net_flow_txn_amt_l3m",
    "min_ca_bal_l6m", "total_bal_avg", "total_num_txn", "total_num_txn_l3m"
]

# Define imputed values
values = [
    -57175100683, -56.90701563, -1533.934551, -8174489344, -4089467647, -494, -652, -88,
    -2.58409E+15, -3468, -9, -24722594949, -7.08974E+14, -1.63506E+15, -305, -116.5666667,
    -3376, -1.999999823, -41778658078, -17148608116, -766.8302552, -2.19432E+11, -17770563461,
    -5924312015, -16.95155584, -11.55096707, -10171618444, -18447803048, -1243033176, -75873315018,
    -664, -2551
]


# Create the imputed_dict
imputed_dict = dict(zip(features, values))

# Configure logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


def impute_missing(df):
    """
    Imputes missing values in the DataFrame.

    Args:
        df (pandas.DataFrame): DataFrame with missing values.

    Returns:
        pandas.DataFrame: DataFrame with missing values imputed.
    """
    try:
        df = df.fillna(imputed_dict)
        return df
    except Exception as e:
        logging.error(f"Error in impute_missing function: {e}")
        return None


def predict(data):
    """
    Predicts mortgage scores for the given data.

    Args:
        data (pandas.DataFrame): Input data for prediction.

    Returns:
        pandas.DataFrame: Predictions including scores and rating categories.
    """
    try:
        data = data.copy()
        data.reset_index(inplace=True, drop=True)

        # 1.1. impute missing
        data_pred = impute_missing(data)
        
        ## 1.2 reorder features
        data_pred = data_pred[features]

        # 3. predict
        predicted_probability = bscoreLoanLGBM.predict(data_pred)
        score = 1000 * (1 - predicted_probability)

        predictions = pd.DataFrame({
            'predicted_probability': predicted_probability,
            'score': score
        })

        # 5. Map raw data
        predictions = pd.concat([data, predictions], axis=1)

        return predictions
    except Exception as e:
        logging.error(f"Error in predict function: {e}")
        return None
        