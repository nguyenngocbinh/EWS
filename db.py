import pyodbc

class Database:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        driver = '{ODBC Driver 17 for SQL Server}'
        server = 'VM-DC-JUMPSRV77\\IFRS9'
        database = 'BSCORE_CREDITCARD'
        username = 'binh_test'
        password = 'Tpb@2023'
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close(self):
        self.conn.close()