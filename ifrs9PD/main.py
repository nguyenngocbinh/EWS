import unittest
from src.db import SQLServerConnector
from src.utils import get_date_ranges
from predictRetail import predictRetail
from test import load_tests  

if __name__ == "__main__":
    # Run the unittests
    test_suite = load_tests()
    test_result = unittest.TextTestRunner(verbosity=2).run(test_suite)

    # Check if all tests passed
    if test_result.wasSuccessful():
        # If tests pass, continue with predictRetail function
        predictRetail()
    else:
        print("Some tests did not pass. Skipping predictRetail execution.")
