import unittest
from src.db import SQLServerConnector
from src.utils import get_date_ranges

class test_ifrs9_ctr_od(unittest.TestCase):

    def setUp(self):
        self.db = SQLServerConnector()
        self.db.connect()
        self.table_name = 'IFRS9_CTR_OD'
        
        # Get the end date of the last month
        self.start_date, self.end_date = get_date_ranges() 
        
    def test_process_date(self):
        # check length of data
        query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0],2, "PROCESS_DATE is not enough length" )

    def test_not_null(self):        
        # Loop through the columns and assert that they are not null        
        columns_to_check = ['PROCESS_DATE','CUSTOMER_ID','CUS_SEGMENT','PRODUCT_GROUP','DISBURSEMENT_DATE','PRINCIPAL_OS_AMOUNT','DPD','NHOM_NO_CONTRACT','MAX_DPD'
        ,'OVD_YN_STATUS','OVD_30D_YN_STATUS','CUSTOMER_INDUSTRY','EXPIRY_DATE','OS_AMOUNT','OUTSTANDING_AVG_MONTH','PRINCIPAL_PAYMENT_FREQUENCY','OVD_PRIN_START_DATE','OVD_INT_START_DATE',
        'DISBURSEMENT_AMOUNT','LIMIT_AMT','UTILIZATION_RATE','LTV','LVT_DISBURSEMENT','WRITE_OFF_TAG','EARLY_REDEMPTION_FLAG']

        for column in columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be null")            

    def test_primary_key(self):
        # check length of data
        query = f''' 
            SELECT CUSTOMER_ID, CONTRACT_ID, PROCESS_DATE, count(1)
            FROM {self.table_name}
            where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'
            group by CUSTOMER_ID, CONTRACT_ID, PROCESS_DATE
            having count(1) > 1
            '''
        result = self.db.execute_query(query)        
        self.assertEqual(len(result), 0, "CUSTOMER_ID, CONTRACT_ID, PROCESS_DATE is not unique" )

    def test_zero(self):        
        columns_to_check = ['DISBURSEMENT_AMOUNT','LIMIT_AMT','UTILIZATION_RATE']

        for column in columns_to_check:    
            query = f'''
                    select PROCESS_DATE, sum({column})
                        from {self.table_name}
                        where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'
                        group by PROCESS_DATE
                        having sum({column}) = 0
            '''                                 
            result = self.db.execute_query(query)
            self.assertEqual(len(result), 0, f"all value in {column} is zero at one or some months between '{self.start_date}' and '{self.end_date}'")          

    def test_negative(self):        
        columns_to_check = ['OS_AMOUNT','OUTSTANDING_AVG_MONTH','PRINCIPAL_OS_AMOUNT','INTEREST_PENALTY_AMOUNT','LIMIT_AMT']

        for column in columns_to_check:    
            query = f"select count(*) from {self.table_name} where {column} < 0 and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be negative")       

    def test_customer_id(self): 
        query = f"select count(1) from {self.table_name} where len(customer_id) <> 8 and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 0, "len(customer_id) <> 8" )       


    def tearDown(self):
        # Close the connection and cursor
        self.db.close()