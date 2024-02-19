import unittest
from testcases import (
    test_mis076,
    test_visa028,
    test_ifrs9_dep_amt_txn,
    test_ifrs9_cust_info,
    test_visa031_sv,
    test_ifrs9_ctr_cl_cl020,
    test_ifrs9_ctr_od
)

if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()

    # Load and add test cases from each module to the suite
    test_loaders = [
        test_mis076,
        test_visa028,
        test_ifrs9_dep_amt_txn,
        test_ifrs9_cust_info,
        test_visa031_sv,
        test_ifrs9_ctr_cl_cl020,
        test_ifrs9_ctr_od
    ]

    for test_loader in test_loaders:
        test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(test_loader))

    # Run the tests
    unittest.TextTestRunner(verbosity=2).run(test_suite)
