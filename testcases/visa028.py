import unittest
from db import Database


class test_visa028(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.db.connect()
        self.table_name = 'VISA028_CLEAN'
        self.start_date = "'2018-01-31'"
        self.end_date = "'2022-12-31'"

    def test_process_date(self):
        # check length of data
        query = f'''select count(distinct PROCESS_DATE)  
                    from {self.table_name} 
                    where PROCESS_DATE between {self.start_date} and {self.end_date}'''    
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 60, "PROCESS_DATE is not enough length" )
             
    def test_not_null(self):        
        # Loop through the columns and assert that they are not null        
        columns_to_check = ['PROCESS_DATE','CUSTOMER_ID','CLIENT_CODE', 'card_number', 'customer_id', 'CARD_TYPE', 'TYPE',
                            'OD_COUNT_DAYS','NHOM_NO_NEW', 'Duno_vuotHM','Duno','CREDIT_LIMIT']

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
            SELECT CUSTOMER_ID, CARD_NUMBER, PROCESS_DATE, count(1)
            FROM {self.table_name}
            where PROCESS_DATE between {self.start_date} and {self.end_date}
            group by CUSTOMER_ID, CARD_NUMBER, PROCESS_DATE
            having count(1) > 1
            '''
        result = self.db.execute_query(query)        
        self.assertEqual(len(result), 0, "CUSTOMER_ID, CARD_NUMBER, PROCESS_DATE is not unique" )

    def test_zero(self):        
        columns_to_check = ['CREDIT_LIMIT','OD_COUNT_DAYS','Duno_vuotHM','Duno']

        for column in columns_to_check:    
            query = f'''
                    select PROCESS_DATE, sum({column})
                        from {self.table_name}
                        where PROCESS_DATE between {self.start_date} and {self.end_date}
                        group by PROCESS_DATE
                        having sum({column}) = 0
            '''                                 
            result = self.db.execute_query(query)
            self.assertEqual(len(result), 0, f"all value in {column} is zero at one or some months between {self.start_date} and {self.end_date}")          

    def test_negative(self):        
        columns_to_check = ['CREDIT_LIMIT','OD_COUNT_DAYS','Duno_saoke', 'Duno']

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
