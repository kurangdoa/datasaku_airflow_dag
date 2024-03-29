import sqlalchemy
from sqlalchemy.sql import text
import pandas as pd

class sqlalchemy_class():
    def __init__(self, username, password=None, host='localhost', port=5432, database='postgres'):
        self.sqlalchemy_username = username
        self.sqlalchemy_password = password
        self.sqlalchemy_host = host
        self.sqlalchemy_port = port
        self.sqlalchemy_database = database
        self.engine = sqlalchemy.create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}', max_overflow=-1, pool_size=100, pool_timeout=300)

    def show(self):
        print("username is", self.sqlalchemy_username)
        # print("password is", self.sqlalchemy_password)
        print("host is", self.sqlalchemy_host)
        print("port is", self.sqlalchemy_port)
        print("database is", self.sqlalchemy_database)

    def pandas_to_sql(self, df, table_name, schema_name, if_exists_remark='fail'):
        import time
        try:
            with self.engine.connect() as conn:
                check = df.to_sql(table_name, con=conn, if_exists=if_exists_remark, index= False, schema = schema_name, chunksize=100000, method='multi')
                time.sleep(10)
                if check == None:
                    info = "Table creation failed"
                else:
                    info = "Table creation success"
        except:
            info = "Table creation failed"
        return print(info)
    
    def execute_create_database(self, database_name):
        try:
            with self.engine.connect() as conn:
                sql = f"""CREATE DATABASE {database_name}"""
                conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(sql))
                info = "Database created successfully"
                conn.close()
        except:
            info = "Database creation failed / Database already exist"
        return print(info)
    
    def execute_drop_database(self, database_name):
        try:
            with self.engine.connect() as conn:
                sql = f"""DROP DATABASE IF EXISTS {database_name};"""
                conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(sql))
                info = "Database deleted successfully"
                conn.close()
        except:
            info = "Database deletion failed"
        return print(info)

    def execute_query(self, query):
        try:
            with self.engine.connect() as conn:
                conn.execute(query)
                info ="Query executed successfully"
                conn.close()
        except:
            info = "Query execution failed"
        return print(info)

    def sql_to_pandas(self, query):
        with self.engine.connect() as conn:
            query = conn.execute(text(query))         
            df = pd.DataFrame(query.fetchall())
            conn.close()
        return df
