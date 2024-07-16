
def calculate_psi(expected, actual, categorical=False, bins=None):
    """
    Calculate the Population Stability Index (PSI) between two sets of data.

    Args:
        expected (numpy.ndarray): The expected values.
        actual (numpy.ndarray): The actual values to be compared against the expected values.
        categorical (bool, optional): Set to True if the variables are categorical. Default is False.
        bins (int, optional): Number of bins for binning numeric data. Default is None (auto).

    Returns:
        float: The calculated PSI value.
    """
    # Check if both arrays are not empty
    if expected.size == 0 or actual.size == 0:
        return 0  # PSI is 0 for empty arrays

    # Check if the variables are categorical
    if categorical:
        # Get unique categories
        categories = np.unique(np.concatenate([expected, actual]))

        # Issue a warning if the number of unique categories exceeds 20
        if len(categories) > 20:
            warnings.warn("Warning: Number of unique categories exceeds 20.")

        # Calculate the expected and actual proportions for each category
        expected_probs = np.array([np.sum(expected == cat) for cat in categories]) / len(expected)
        actual_probs = np.array([np.sum(actual == cat) for cat in categories]) / len(actual)
    else:
        # Convert input arrays to NumPy arrays
        expected = np.asarray(expected)
        actual = np.asarray(actual)

        # Handle NaN values in the input arrays
        expected = expected[~np.isnan(expected)]
        actual = actual[~np.isnan(actual)]
        # Apply binning for numeric columns
        if bins is None:
            bins = 10  # Default to 10 bins, you can change this value as needed

        # Calculate the bin edges based on percentiles
        # bin_edges = np.percentile(np.hstack((expected, actual)), np.linspace(0, 100, bins + 1))
        bin_edges = np.percentile(expected, np.linspace(0, 100, bins + 1))

        # Calculate the expected and actual proportions for each bin
        expected_probs, _ = np.histogram(expected, bins=bin_edges)
        actual_probs, _ = np.histogram(actual, bins=bin_edges)

        # Normalize to get proportions
        expected_probs = expected_probs / len(expected)
        actual_probs = actual_probs / len(actual)
        
    # Initialize PSI
    psi_value = 0

    # Loop over each bin or category
    for i in range(len(expected_probs)):
        # Avoid division by zero and log of zero
        if expected_probs[i] == 0 or actual_probs[i] == 0:
            continue
        # Calculate the PSI for this bin or category
        psi_value += (expected_probs[i] - actual_probs[i]) * np.log(expected_probs[i] / actual_probs[i])

    return psi_value


def calculate_psi_for_features(df1, df2, features, categorical_threshold=20, bins=None):
    """
    Calculate the Population Stability Index (PSI) for selected features between two DataFrames.

    Args:
        df1 (pandas.DataFrame): First DataFrame.
        df2 (pandas.DataFrame): Second DataFrame.
        features (list): List of feature names to calculate PSI.
        categorical_threshold (int, optional): Threshold to consider a variable as categorical. Default is 20.
        bins (int or sequence of scalars, optional): If numeric, number of bins for binning the data. Default is None (auto).

    Returns:
        pandas.DataFrame: DataFrame with calculated PSI values for each selected feature.
    """
    psi_results = []

    for feature in features:
        if feature not in df1.columns or feature not in df2.columns:
            raise ValueError(f"Feature '{feature}' not found in both DataFrames.")

        expected = df1[feature]
        actual = df2[feature]

        # Filter out None values from expected and actual
        expected_filtered = expected[~pd.isnull(expected)]
        actual_filtered = actual[~pd.isnull(actual)]

        # Determine if the feature is categorical based on unique values
        if len(np.unique(expected_filtered)) <= categorical_threshold:
            categorical = True
        else:
            categorical = False

        psi_value = calculate_psi(expected_filtered, actual_filtered, categorical=categorical, bins=bins)

        psi_results.append({
            'Feature': feature,
            'PSI': psi_value
        })

    return pd.DataFrame(psi_results)
    