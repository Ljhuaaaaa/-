import pymssql
import pandas as pd

class Conn_database:
    '''链接数据库，取出对应sql的数据'''
    def __init__(self):
        self.conn = pymssql.connect(host='******'
                                    ,user='******'
                                    ,password='******'
                                    ,database='******')


    def read_sql_data(self,sql):
        '''根据sql，抽取数据'''
        return pd.read_sql(sql,self.conn)

