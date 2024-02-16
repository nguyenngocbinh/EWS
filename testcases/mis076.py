import unittest
from db import Database


class test_mis076(unittest.TestCase):

    def setUp(self):
        self.db = Database()
        self.db.connect()
        self.table_name = 'MIS076'
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
        columns_to_check = ['branch_code', 'card_number', 'customer_id', 'transaction_type', 'mcc', 'transaction_date']

        for column in columns_to_check:    
            query = f'''SELECT COUNT(*) 
                        FROM {self.table_name} 
                        WHERE {column} IS NULL 
                        and PROCESS_DATE between {self.start_date} and {self.end_date}'''                               
            result = self.db.execute_query(query)
            self.assertEqual(result[0][0], 0, f"{column} should be not null")            
        
    def test_amount_vnd(self):
        query =  f'''
                select PROCESS_DATE, count(1)
                    from {self.table_name}
                    where amount_vnd is null
                    and amount > 0
                    group by PROCESS_DATE
                    order by PROCESS_DATE
                  '''
        result = self.db.execute_query(query)
        print(result[:5])
        self.assertEqual(result[0][1], 0, "amount_vnd should not be null")
    
    def test_mcc(self): 
        query = f'''select count(1) 
                    from {self.table_name} 
                    where len(mcc) <> 8 
                    and PROCESS_DATE between {self.start_date} and {self.end_date}'''    
        result = self.db.execute_query(query)        
        self.assertEqual(result[0][0], 0, "len(mcc) <> 4" )

    # 2018-03-31, 2018-05-31 contain NULL
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
        print(result[:5])
        self.assertEqual(len(result), 0, "transaction_date is not enough date")

    def tearDown(self):
        # Close the connection and cursor
        self.db.close()

