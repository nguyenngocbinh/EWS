import unittest
from db import Database


class test_ifrs9_cust_info(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.db.connect()
        self.table_name = 'IFRS9_CUST_INFO'
        self.start_date = "'2022-12-31'"
        self.end_date = "'2022-12-31'"

    def test_process_date(self):
        # check length of data
        query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between {self.start_date} and {self.end_date}"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 1, "PROCESS_DATE is not enough length" )

    def test_customer_id(self): 
        query = f'''select count(1) 
                    from {self.table_name} 
                    where len(customer_id) <> 8 
                    and PROCESS_DATE between {self.start_date} and {self.end_date}'''    
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 0, "len(customer_id) <> 8" )

    def test_not_null(self):        
        # Loop through the columns and assert that they are not null
        columns_to_check = ['PROCESS_DATE',
                            'CUSTOMER_ID',                                                                                    
                            #'MTH_IN_REL',
                            #'YR_IN_REL',
                            'AGE']

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

    # khong check khach hang > 80, vi du lieu nay co tat ca cac KH
    def test_outlier_age(self):    
        query = f'''SELECT COUNT(*) FROM {self.table_name} 
                    WHERE PROCESS_DATE between {self.start_date} and {self.end_date}
                    AND AGE < 18 '''                               
        result = self.db.execute_query(query)
        self.assertEqual(result[0][0], 0, "AGE should be >= 18")
    
    def tearDown(self):
        # Close the connection and cursor
        self.db.close()
