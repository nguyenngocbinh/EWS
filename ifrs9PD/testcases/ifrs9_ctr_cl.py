import unittest
from src.db import SQLServerConnector
from src.utils import get_date_ranges

class test_ifrs9_ctr_cl(unittest.TestCase):

    def setUp(self):
        self.db = SQLServerConnector()
        self.db.connect()
        self.table_name = 'IFRS9_CTR_CL'
        
        # Get the end date of the last month
        self.start_date, self.end_date = get_date_ranges()       
        
    def test_process_date(self):
        # check length of data
        query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 2, "PROCESS_DATE is not enough length" )

    def test_not_null(self):        
        # Loop through the columns and assert that they are not null
        columns_to_check = ['PROCESS_DATE',
                            'CUSTOMER_ID',
                            'ACCOUNT_STATUS',
                            'ALTERNATIVE_ACCOUNT_NUMBER'
                            ]

        for column in columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                               
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be null")        

    def test_len_customer_id(self):
        query = f'''
                SELECT len(customer_id), count(1) from {self.table_name}
                where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'
                group by len(customer_id)
                having len(customer_id) <> 8
                '''   
        result = self.db.execute_query(query)        
        self.assertEqual(len(result), 0, "len(customer_id) <> 8" )
    
    def tearDown(self):
        # Close the connection and cursor
        self.db.close()
