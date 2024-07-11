import unittest
from src.db import SQLServerConnector
from src.utils import get_date_ranges


class test_mis076(unittest.TestCase):
    
    def setUp(self):
        self.db = SQLServerConnector()
        self.db.connect()
        self.table_name = 'MIS076'
        
        # Get the end date of the last month
        self.start_date, self.end_date = get_date_ranges()
        
    def test_process_date(self):
        # check length of data
        query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 60, "PROCESS_DATE is not enough length" )

    def test_not_null(self):        
        # Loop through the columns and assert that they are not null
        columns_to_check = ['ma_cn', 'card_number', 'cif', 'transaction_date', 'transaction_type', 'amount', 
                            'amount_vnd', 'gd_online', 'mcc_category', 'creation_date', 'hinh_thuc_giao_dich']

        for column in columns_to_check:    
            query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL and PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"                               
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should not be null")            
    
    # def test_primary_key(self):
    #     # check length of data
    #     query = f"select count(distinct PROCESS_DATE) cnt from {self.table_name} where PROCESS_DATE between '{self.start_date}' and '{self.end_date}'"        
    #     result = self.db.execute_query(query)        
    #     self.assertEqual(result[0][0], 60, "PROCESS_DATE is not enough length" )

    def test_transaction_date(self):
        query = f''' 
                with DateRange
                as (
                    select CAST({self.start_date} as date) as date
                    
                    union all
                    
                    select DATEADD(day, 1, date)
                    from DateRange
                    where date <= {self.end_date}
                    )
                    ,tmp
                as (
                    select distinct transaction_date
                    from {self.table_name}
                    where PROCESS_DATE between {self.start_date} 
                            and {self.end_date}
                    )
                select dr.date
                from DateRange dr
                left join tmp t on dr.date = t.transaction_date
                where t.transaction_date is null
                order by dr.date
                option (maxrecursion 5000)

                '''
        result = self.db.execute_query(query)
        self.assertEqual(result[0][0], 0, f"{column} should not be null")

    def tearDown(self):
        # Close the connection and cursor
        self.db.close()

