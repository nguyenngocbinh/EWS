import unittest
from db import Database


class test_r18(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.db.connect()
        self.table_name = 'R18'
        self.start_date = "'2018-01-31'"
        self.end_date = "'2022-12-31'"

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
        columns_to_check = ['CUSTOMER_ID', 'MACIC', 'NHOMCAONHATTAITCTDKHAC', 'NHOMNOTAITCTDHOITIN', 'DUNOTAITCTDHOITIN']

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
   

    def tearDown(self):
        # Close the connection and cursor
        self.db.close()

