import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score
from scipy.stats import ks_2samp
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
class PortfolioPerformance:
    """
    A class to calculate GINI and KS metrics for different portfolios over a date range, filtering only the end of month dates.
    """

    def __init__(self, df):
        """
        Initializes the PortfolioPerformance class with a DataFrame.

        Parameters:
        df (pd.DataFrame): DataFrame containing 'process_date', 'portfolio', 'score', and 'default_flag' columns.
        """
        self.df = df.copy()
        self.df['process_date'] = pd.to_datetime(self.df['process_date'], errors='coerce')
    
    def _filter_data_by_end_of_month(self, start_date, end_date):
        """
        Filters the DataFrame to include only the rows where 'process_date' is the end of the month within the specified date range.

        Parameters:
        start_date (str): The start date of the range.
        end_date (str): The end date of the range.

        Returns:
        pd.DataFrame: Filtered DataFrame.
        """
        mask = (self.df['process_date'] >= start_date) & (self.df['process_date'] <= end_date)
        df_filtered = self.df[mask]

        # Find the last day of each month within the filtered date range
        end_of_month_dates = df_filtered['process_date'].dt.to_period('M').drop_duplicates().dt.to_timestamp('M')
        df_filtered = df_filtered[df_filtered['process_date'].isin(end_of_month_dates)]

        # Log a warning if df_filtered is empty
        if df_filtered.empty:
            logger.warning(f"No data found for the end of month within the date range {start_date} to {end_date}.")
        self.df_filtered = df_filtered
        return df_filtered
    
    def _calculate_metrics(self, subset):
        """
        Calculates the GINI and KS metrics for a given subset of data.

        Parameters:
        subset (pd.DataFrame): DataFrame subset for a specific portfolio.

        Returns:
        tuple: GINI coefficient, KS statistic, and KS p-value.
        """

        # Remove rows with NaN values in 'default_flag' or 'score'
        subset = subset.dropna(subset=['default_flag', 'score'])

        # Convert 'default_flag' to integers
        y_true = subset['default_flag'].astype(int)
        y_pred = 1000 - pd.to_numeric(subset['score'], errors='coerce')

        unique_classes = y_true.nunique()

        if unique_classes < 2:
            logger.warning(f"Only one class present in y_true for portfolio {subset['portfolio'].iloc[0] if not subset.empty else 'unknown'}. GINI and KS metrics cannot be calculated.")
            return np.nan, np.nan, np.nan
        
        if y_true.isin([0, 1]).all() and not y_true.empty and not y_pred.empty:

            gini = 2 * roc_auc_score(y_true, y_pred) - 1 if len(y_true) > 0 else np.nan 
            
            predict_1 = y_pred[y_true == 1]
            predict_0 = y_pred[y_true == 0]
            
            if len(predict_1) > 1 and len(predict_0) > 1:
                ks_statistic, ks_pvalue = ks_2samp(predict_1, predict_0)                               
            else:
                ks_statistic, ks_pvalue = np.nan, np.nan                
        else:
            ks_statistic, ks_pvalue = np.nan, np.nan
            gini = np.nan
        
        return gini, ks_statistic, ks_pvalue

    def calculate_performance(self, start_date, end_date):
        """
        Calculates performance metrics for each portfolio within a date range, considering only the end of month dates.

        Parameters:
        start_date (str): The start date of the range.
        end_date (str): The end date of the range.

        Returns:
        pd.DataFrame: DataFrame containing performance metrics for each portfolio over the date range.
        """
        logger.info(f"Calculating performance for date range {start_date} to {end_date}.")
        df_filtered = self._filter_data_by_end_of_month(start_date, end_date)
        unique_dates = df_filtered['process_date'].unique()
        
        results = []
        
        for process_date in unique_dates:
            df_date_filtered = df_filtered[df_filtered['process_date'] == process_date]
            unique_portfolios = df_date_filtered['portfolio'].unique()
            
            for portfolio in unique_portfolios:
                subset = df_date_filtered[df_date_filtered['portfolio'] == portfolio].sort_values(by='score', ascending=False)
                gini, ks_statistic, ks_pvalue = self._calculate_metrics(subset)
                
                results.append({
                    'process_date': process_date,
                    'portfolio': portfolio,
                    'gini': gini,
                    'ks': ks_statistic,
                    'ks_pvalue': ks_pvalue
                })
        
        result_df = pd.DataFrame(results)
        logger.info("Performance calculation completed.")
        return result_df