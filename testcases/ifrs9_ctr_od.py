import unittest
from db import Database


class test_IFRS9_CTR_OD(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.db.connect()
    
        self.table_name = 'IFRS9_CTR_OD'
        self.start_date = "'2018-01-31'"
        self.end_date = "'2022-12-31'"

    def test_process_date(self):
        # check length of data
        query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between {self.start_date} and {self.end_date}"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 60, "PROCESS_DATE is not enough length" )
             
    def test_not_null(self):        
        # Loop through the columns and assert that they are not null        
        columns_to_check = ['PROCESS_DATE','CUSTOMER_ID','CUS_SEGMENT','CUSTOMER_INDUSTRY','PRODUCT_GROUP','DISBURSEMENT_DATE','EXPIRY_DATE',
        'INTEREST_RATE','OS_AMOUNT','OUTSTANDING_AVG_MONTH','PRINCIPAL_OS_AMOUNT',
'PRINCIPAL_PAYMENT_FREQUENCY','INTERST_PAYMENT_FREQUENCY','DPD','NHOM_NO_CONTRACT','OVD_PRIN_START_DATE','OVD_INT_START_DATE','MAX_DPD',
'DISBURSEMENT_AMOUNT','LIMIT_AMT','UTILIZATION_RATE','LTV','LVT_DISBURSEMENT','WRITE_OFF_TAG','EARLY_REDEMPTION_FLAG','OVD_YN_STATUS',
'OVD_30D_YN_STATUS']


        for column in columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL and PROCESS_DATE between {self.start_date} and {self.end_date}"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be null")            

    def test_primary_key(self):
        # check length of data
        query = f''' 
            SELECT CUSTOMER_ID, CONTRACT_ID, PROCESS_DATE, count(1)
            FROM {self.table_name}
            where PROCESS_DATE between {self.start_date} and {self.end_date}
            group by CUSTOMER_ID, CONTRACT_ID, PROCESS_DATE
            having count(1) > 1
            '''
        result = self.db.execute_query(query)        
        self.assertEqual(len(result), 0, "CUSTOMER_ID, CONTRACT_ID, PROCESS_DATE is not unique" )
     

    def test_negative(self):        
        columns_to_check = ['INTEREST_PENALTY_AMOUNT','LIMIT_AMT']

        for column in columns_to_check:    
            query = f"select count(*) from {self.table_name} where {column} < 0 and PROCESS_DATE between {self.start_date} and {self.end_date}"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be negative")       

    def test_customer_id(self): 
        query = f"select count(1) from {self.table_name} where len(customer_id) <> 8 and PROCESS_DATE between {self.start_date} and {self.end_date}"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 0, "len(customer_id) <> 8" )       


    def tearDown(self):
        # Close the connection and cursor
        self.db.close()
