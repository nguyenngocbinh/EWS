import yaml
import pandas as pd

def format_data_types(data, data_types):
    """
    Format the data types of the DataFrame to align with the target table's data types.

    Parameters:
    - data (DataFrame): Source data to be formatted.
    - data_types (dict): Dictionary containing target table column names and their corresponding data types.

    Returns:
    - DataFrame: Formatted DataFrame.
    
    Example:
    ```python
    # Define data types for target table columns
    data_types = {'column1': 'int', 'column2': 'float', 'column3': 'datetime'}
    
    # Example DataFrame
    import pandas as pd
    data = pd.DataFrame({'column1': [1, 2, 3], 'column2': [1.1, 2.2, 3.3], 'column3': ['2024-01-01', '2024-01-02', '2024-01-03']})
    
    # Format data types
    formatted_data = format_data_types(data, data_types)
    
    print(formatted_data.dtypes)
    ```
    """     
    for col, dtype in data_types.items():
        if col in data.columns:
            data[col] = data[col].astype(dtype)
    return data

def load_config(config_file):
    """Load configuration from YAML file."""
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config