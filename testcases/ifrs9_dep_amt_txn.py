import unittest
from db import Database


class test_ifrs9_dep_amt_txn(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.db.connect()
        self.table_name = 'IFRS9_DEP_AMT_TXN'
        self.start_date = "'2018-01-31'"
        self.end_date = "'2022-12-31'"
        self.columns_to_check = ['TXN_AMT'
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
        query = f'''select count(distinct PROCESS_DATE)  
                    from {self.table_name} 
                    where PROCESS_DATE between {self.start_date} and {self.end_date}'''    
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 60, "PROCESS_DATE is not enough length" )
        
    def test_customer_id(self): 
        query = f'''select count(1) 
                    from {self.table_name} 
                    where len(customer_id) <> 8 
                    and PROCESS_DATE between {self.start_date} and {self.end_date}'''    
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 0, "len(customer_id) <> 8" )

    def test_not_null(self):        
        # Loop through the columns and assert that they are not null       

        for column in columns_to_check:    
            query = f'''SELECT COUNT(*) 
                        FROM {self.table_name} 
                        WHERE {column} IS NULL 
                        and PROCESS_DATE between {self.start_date} and {self.end_date}'''                               
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should be not null")     

    def test_primary_key(self):
        # check length of data
        query = f''' 
            SELECT [CUSTOMER_ID], [PROCESS_DATE], count(1)
            FROM {self.table_name}
            where PROCESS_DATE between {self.start_date} and {self.end_date}
            group by [CUSTOMER_ID], [PROCESS_DATE]
            having count(1) > 1
            '''
        result = self.db.execute_query(query)        
        self.assertEqual(len(result), 0, "PROCESS_DATE, CUSTOMER_ID is not unique" )

    def test_zero(self):
        # check length of data
        for column in self.columns_to_check:    
            query = f'''
                    select PROCESS_DATE, sum({column})
                        from {self.table_name}
                        where PROCESS_DATE between {self.start_date} and {self.end_date}
                        group by PROCESS_DATE
                        having sum({column}) = 0
            '''                                 
            result = self.db.execute_query(query)
            self.assertEqual(len(result), 0, f"all value in {column} is zero at one or some months between {self.start_date} and {self.end_date}")   


    def tearDown(self):
        # Close the connection and cursor
        self.db.close()
