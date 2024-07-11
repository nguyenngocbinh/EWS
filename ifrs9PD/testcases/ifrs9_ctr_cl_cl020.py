import unittest
from src.db import SQLServerConnector
from src.utils import get_date_ranges

class test_ifrs9_ctr_cl_cl020(unittest.TestCase):

    def setUp(self):
        self.db = SQLServerConnector()
        self.db.connect()
        self.table_name = 'IFRS9_CTR_CL_CL020'
        
        # Get the end date of the last month
        self.start_date, self.end_date = get_date_ranges()

    def test_process_date(self):
        query = f"SELECT COUNT(DISTINCT PROCESS_DATE) cnt FROM {self.table_name} WHERE PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 60, "The count of distinct PROCESS_DATE is not 60")

    def test_not_null(self):
        columns_to_check = [
            'PROCESS_DATE', 'CUSTOMER_ID', 'CUS_SEGMENT', 'CUSTOMER_INDUSTRY', 
            'PRODUCT_GROUP', 'DISBURSEMENT_DATE', 'EXPIRY_DATE', 'OS_AMOUNT', 
            'OUTSTANDING_AVG_MONTH', 'PRINCIPAL_OS_AMOUNT', 'PRINCIPAL_PAYMENT_FREQUENCY', 
            'INTEREST_PAYMENT_FREQUENCY', 'DPD', 'NHOM_NO_CONTRACT', 'OVD_PRIN_START_DATE', 
            'OVD_INT_START_DATE', 'MAX_DPD', 'DISBURSEMENT_AMOUNT', 'LIMIT_AMT', 
            'UTILIZATION_RATE', 'LTV', 'LVT_DISBURSEMENT', 'WRITE_OFF_TAG', 
            'EARLY_REDEMPTION_FLAG', 'OVD_YN_STATUS', 'OVD_30D_YN_STATUS'
        ]

        for column in columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL AND PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be null")

    def test_primary_key(self):
        query = f""" 
            SELECT CUSTOMER_ID, CONTRACT_ID, PROCESS_DATE, LIMIT_ID, COUNT(1)
            FROM {self.table_name}
            WHERE PROCESS_DATE between '{self.start_date}' and '{self.end_date}'
            GROUP BY CUSTOMER_ID, CONTRACT_ID, LIMIT_ID, PROCESS_DATE
            HAVING COUNT(1) > 1
        """
        result = self.db.execute_query(query)        
        self.assertEqual(len(result), 0, "CUSTOMER_ID, CONTRACT_ID, LIMIT_ID, PROCESS_DATE is not unique")

    def test_zero(self):
        columns_to_check = ['DISBURSEMENT_AMOUNT', 'LIMIT_AMT', 'UTILIZATION_RATE']

        for column in columns_to_check:    
            query = f"""
                SELECT PROCESS_DATE, SUM({column})
                FROM {self.table_name}
                WHERE PROCESS_DATE between '{self.start_date}' and '{self.end_date}'
                GROUP BY PROCESS_DATE
                HAVING SUM({column}) = 0
            """
            result = self.db.execute_query(query)
            self.assertEqual(len(result), 0, f"All values in {column} are zero at one or more months between '{self.start_date}' and '{self.end_date}'")

    def test_negative(self):
        columns_to_check = ['OS_AMOUNT', 'OUTSTANDING_AVG_MONTH', 'PRINCIPAL_OS_AMOUNT', 'INTEREST_PENALTY_AMOUNT', 'LIMIT_AMT']

        for column in columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} < 0 AND PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be negative")

    def test_customer_id(self):
        query = f"SELECT COUNT(1) FROM {self.table_name} WHERE LEN(CUSTOMER_ID) <> 8 AND PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 0, "Length of CUSTOMER_ID is not 8")

    def tearDown(self):
        self.db.close()

