import unittest
from src.db import SQLServerConnector
from src.utils import get_date_ranges


class test_cl020(unittest.TestCase):
    
    def setUp(self):
        self.db = SQLServerConnector()
        self.db.connect()
        self.table_name = 'CL020'        
        self.start_date, self.end_date = get_date_ranges()

    def test_process_date(self):
        # check length of data
        query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 2, "PROCESS_DATE is not enough length" )
             
    def test_not_null(self):        
        # Loop through the columns and assert that they are not null        
        columns_to_check = ['PROCESS_DATE','CUSTOMER_ID','DISBURSEMENT_AMOUNT','LCY_OS_AMOUNT','NHOM_NO_CONTRACT','OD_COUNT_DAY','APPROVAL_DATE']

        for column in columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be null")            

    def test_primary_key(self):
        # check primary key
        query = f''' 
            SELECT CUSTOMER_ID, CONTRACT_ID, PROCESS_DATE, count(1)
            FROM {self.table_name}
            where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'
            group by CUSTOMER_ID, CONTRACT_ID, PROCESS_DATE
            having count(1) > 1
            '''
        result = self.db.execute_query(query)        
        self.assertEqual(len(result), 0, "CUSTOMER_ID, CONTRACT_ID , PROCESS_DATE is not unique" )


    def test_customer_id(self): 
        query = f"select count(1) from {self.table_name} where len(customer_id) <> 8 and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 0, "len(customer_id) <> 8" )       

    def tearDown(self):
        # Close the connection and cursor
        self.db.close()
