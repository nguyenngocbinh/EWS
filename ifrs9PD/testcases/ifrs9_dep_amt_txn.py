import unittest
from src.db import SQLServerConnector
from src.utils import get_date_ranges


class test_ifrs9_dep_amt_txn(unittest.TestCase):
    
    def setUp(self):
        self.db = SQLServerConnector()
        self.db.connect()
        self.table_name = 'IFRS9_DEP_AMT_TXN'
        
        # Get the end date of the last month
        self.start_date, self.end_date = get_date_ranges()

        self.columns_to_check = ['PROCESS_DATE'
                            ,'CUSTOMER_ID'
                            ,'TXN_AMT'
                            ,'TXN_AMT_L3M'
                            ,'AVG_TXN_AMT_L1M'
                            ,'AVG_TXN_AMT_L3M'
                            ,'AVG_TXN_AMT_L1M_VS_L3M'
                            ,'CASH_TXN_AMT'
                            ,'CASH_TXN_AMT_L3M'
                            ,'AVG_CASH_VS_TXN_AMT_L1M'
                            ,'AVG_CASH_VS_TXN_AMT_L3M'
                            ,'AVG_CASH_TXN_AMT_PER_TIME_L3M'
                            ,'EBANK_TXN_AMT'
                            ,'AVG_EBANK_TXN_AMT_L3M'
                            ,'AVG_SAL_TXN_AMT_L3M'
                            ,'MIN_SAL_TXN_AMT_L3M'
                            ,'CASH_CR_TXN_AMT'
                            ,'CASH_CR_TXN_AMT_L3M'
                            ,'CASH_DR_TXN_AMT'
                            ,'CASH_DR_TXN_AMT_L3M'
                            ,'AVG_CASH_CR_TXN_AMT_L3M'
                            ,'AVG_CASH_DR_TXN_AMT_L3M'
                            ,'CR_TXN_AMT'
                            ,'CR_TXN_AMT_L3M'
                            ,'DR_TXN_AMT'
                            ,'DR_TXN_AMT_L3M'
                            ,'NET_FLOW_TXN_AMT'
                            ,'MAX_NET_FLOW_TXN_AMT_L3M'
                            ,'AVG_CASH_CR_TXN_AMT_COVR_L3M'
                            ,'AVG_CR_TXN_AMT_VS_OS_L3M'
                            ,'SERVICE_AMT'
                            ,'MIN_SERVICE_AMT_L3M'
                            ,'NUM_CASH_TXN'
                            ,'NUM_NON_CASH_TXN'
                            ,'TOTAL_NUM_TXN'
                            ,'TOTAL_NUM_TXN_L3M'
                            ,'TOTAL_NUM_NON_CASH_TXN_L3M'
                            ,'AVG_NUM_NON_CASH_TXN_CVR_L3M']

    def test_process_date(self):
        # check length of data
        query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 2, "PROCESS_DATE is not enough length" )

    def test_zero(self):
        # check sum not 0
        for column in  self.columns_to_check:    
           query = f'''
            select sum({column})
            from {self.table_name}
            where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'
            having sum({column})= 0
            '''   
        result = self.db.execute_query(query)        
        self.assertEqual(sum(result), 0, f"{column}= 0" )

    def test_not_null(self):        
        # Loop through the columns and assert that they are not null
        for column in  self.columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                               
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be null")            
    
    def tearDown(self):
        # Close the connection and cursor
        self.db.close()