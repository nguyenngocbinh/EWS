import unittest
from testcases.visa028 import test_visa028
from testcases.ifrs9_dep_amt_txn import test_ifrs9_dep_amt_txn
from testcases.ifrs9_cust_info import test_ifrs9_cust_info
from testcases.ifrs9_ctr_cc import test_ifrs9_ctr_cc
from testcases.ifrs9_ctr_cl import test_ifrs9_ctr_cl
from testcases.ifrs9_ctr_od import test_ifrs9_ctr_od
from testcases.cl020 import test_cl020

def load_tests():
    # Create a test suite
    test_suite = unittest.TestSuite()
    test_loader = unittest.TestLoader()

    # Load test cases and add to suite
    test_cases = [
        test_visa028,
        test_ifrs9_dep_amt_txn,
        test_ifrs9_cust_info,
        test_ifrs9_ctr_cc,
        test_ifrs9_ctr_cl,
        test_ifrs9_ctr_od,
        test_cl020
    ]

    for test_case in test_cases:
        test_suite.addTests(test_loader.loadTestsFromTestCase(test_case))

    return test_suite
