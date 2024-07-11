import unittest
from src.db import SQLServerConnector
from src.utils import get_date_ranges

class test_visa028(unittest.TestCase):
    
    def setUp(self):
        self.db = SQLServerConnector()
        self.db.connect()
        self.table_name = 'VISA028_CLEAN'
        
        # Get the end date of the last month
        self.start_date, self.end_date = get_date_ranges()
        
    def test_process_date(self):
        # check length of data
        query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 9, "PROCESS_DATE is not enough length" )

    def test_not_null(self):        
        # Loop through the columns and assert that they are not null
        columns_to_check = ['card_number', 'customer_id', 'branch_code', 'OD_COUNT_DAYS', 'OPENING_DATE', 
                            'EXPIRE_DATE', 'NHOM_NO_NEW', 'Duno', 'TYPE','CREDIT_LIMIT']

        for column in columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be null")               

    def test_zero(self):        
        columns_to_check = ['OD_COUNT_DAYS','NHOM_NO_NEW','Duno_saoke','Duno_vuotHM','Duno','CREDIT_LIMIT']

        for column in columns_to_check:    
           query = f'''
            select sum({column})
            from {self.table_name}
            where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'
            having sum({column})= 0
            '''   
        result = self.db.execute_query(query)        
        self.assertEqual(sum(result), 0, f"{column}= 0" )
      

    def test_negative(self):        
        columns_to_check = ['CREDIT_LIMIT','OD_COUNT_DAYS','Duno_saoke', 'Duno']

        for column in columns_to_check:    
            query = f"select count(*) from {self.table_name} where {column} < 0 and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be negative")       

    def test_len(self):        
        columns_to_check = ['CUSTOMER_ID']

        for column in columns_to_check:    
            query = f"select count(*) from {self.table_name} where len({column}) <> 8 and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                          
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be len <> 8")   

    def tearDown(self):
        # Close the connection and cursor
        self.db.close()
