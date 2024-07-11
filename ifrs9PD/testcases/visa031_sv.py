import unittest
from src.db import SQLServerConnector
from src.utils import get_date_ranges

class test_visa031_sv(unittest.TestCase):
    
    def setUp(self):
        self.db = SQLServerConnector()
        self.db.connect()
        self.table_name = 'VISA031_SV'
        
        # Get the end date of the last month
        self.start_date, self.end_date = get_date_ranges()
        
    def test_process_date(self):
        # check length of data
        query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 60, "PROCESS_DATE is not enough length" )

    def test_not_null(self):        
        # Loop through the columns and assert that they are not null
        columns_to_check = ['process_date',                            
                            'so_giao_dich',
                            'ngay_giao_dich',
                            'so_the',
                            'hang_the',
                            'trang_thai_the',
                            'so_tai_khoan_the',
                            'so_tien_tra_no',
                            'ma_acronym',
                            'ma_gd',
                            'mo_ta_gd',
                            'danh_dau_khoan_reversal',
                            'ma_kh',
                            'cif_kh'                            
                            ]

        for column in columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                               
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be null")            
    
    def tearDown(self):
        # Close the connection and cursor
        self.db.close()
