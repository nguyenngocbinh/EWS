from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def get_date_ranges(date=None):
    """
    Calculate the end dates for the last month and six months ago.

    Parameters:
    - date (datetime, optional): The reference date. If None, defaults to the current date.

    Returns:
    - tuple: A tuple containing 'end_of_six_months_ago' and 'end_of_last_month' in 'YYYY-MM-DD' format.
    """
    if date is None:
        date = datetime.now()
        
    end_of_last_month = (date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
    six_months_ago = date - relativedelta(months=5)
    end_of_six_months_ago = (six_months_ago.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    return end_of_six_months_ago, end_of_last_month
    
def get_end_of_last_month():
    """
    Calculate the end of the last month.

    Returns:
    - str: End of last month in 'YYYY-MM-DD' format.
    """
    return (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
