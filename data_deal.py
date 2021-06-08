import pandas as pd
import numpy as np
from connect_database import Conn_database
from decorator import log_information

class Data_deal:

    def __init__(self):

        self.conn_database = Conn_database()
        #今日
        self.rq1 = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), GETDATE(), 111)").values[0][0]
        #月初
        self.rq2 = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(dd,-DAY(getdate())+1,getdate())	"
                                                        ", 111)").values[0][0]
        #年初
        self.rq3 = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(yy, DATEDIFF(yy,0,getdate()), 0), 111)").values[0][0]
        #去年年初
        self.rq4 = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(YEAR, -1,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)) "
                                                        ", 111)").values[0][0]
        #去年今天
        self.rq5 = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(YEAR, -1, getdate()) , 111)").values[0][0]
        #去年本月月初
        self.rq6 = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(dd,-DAY(DATEADD(YEAR, -1, getdate()))+1"
                                                        ",DATEADD(YEAR, -1, getdate())), 111)").values[0][0]
        #去年本月月末
        self.rq7 = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(dd,-1,DATEADD(MONTH"
                                                        ",+1,DATEADD(dd,-DAY(DATEADD(YEAR, -1, getdate()))+1,DATEADD(YEAR, -1, getdate())))), 111)").values[0][0]
        #去年本季季初
        self.rq8 = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(qq,DATEDIFF(qq,0,DATEADD(YEAR, -1, getdate())),0), 111)").values[0][0]
        #去年本季季末
        self.rq9  = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(dd,-1,DATEADD(qq"
                                                         ",+1,DATEADD(qq,DATEDIFF(qq,0,DATEADD(YEAR, -1, getdate())),0))), 111)").values[0][0]
        #本季季初
        self.rq10  = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(qq,DATEDIFF(qq,0,getdate()),0), 111)").values[0][0]
        #本季季末
        self.rq11  = self.conn_database.read_sql_data(sql="SELECT CONVERT(varchar(100), DATEADD(dd,-1,DATEADD(qq"
                                                          ",+1,DATEADD(qq,DATEDIFF(qq,0,getdate()),0))), 111)").values[0][0]

        #rp_cp13数据仓库
        self.sql_rp_cp13 = "select autoid,pb,db,rq,khhm,je,khsy from mcs.dbo.Rp_CP13  with(nolock) where rq>='''{} '''" \
                           "and rq<='''{}''' and khhm not in('312023','312024','312019','360005','311793','318117','302273'" \
                           ",'312020','312002','360004')".format(self.rq4,self.rq1)

        # rp_cp23数据仓库
        self.sql_rp_cp23 = "select autoid,pb,db,rq,khhm,je,khsy from mcs.dbo.Rp_CP23  with(nolock) where rq>='''{}''' " \
                           "and rq<='''{}''' and khhm not in('312023','312024','312019','360005','311793','318117','302273'" \
                           ",'312020','312002','360004')".format(self.rq4,self.rq1)

        #CP17_G数据仓库
        self.sql_gn_cp17_g = "SELECT rq,je FROM GN.DBO.CP17_G WITH(NOLOCK) WHERE pzlb = '发货金额'"

        self.rp_cp13data = pd.DataFrame()
        self.rp_cp23data = pd.DataFrame()
        self.gn_cp17_g = pd.DataFrame()

        self.data_dict = dict()

    @log_information
    def data_extract(self):
        '''rp_cp13,rp_cp23,gn_cp17_g数据表的数据抽取'''

        #抽取rp_cp13的数据
        self.rp_cp13data = self.conn_database.read_sql_data(self.sql_rp_cp13)

        #抽取rp_cp23的数据
        self.rp_cp23data = self.conn_database.read_sql_data(self.sql_rp_cp23)

        #抽取gn_cp17_g的数据
        self.gn_cp17_g = self.conn_database.read_sql_data(self.sql_gn_cp17_g)

        return self.rp_cp13data,self.rp_cp23data,self.gn_cp17_g

    @log_information
    def data_handle(self):
        '''通过数据整合出销售开单报表的dataframe'''

        # 1.经销
        #本日
        jingxiaotoday_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                    & ((self.rp_cp13data.khhm.str.strip().str.len()==6)
                                    | ((self.rp_cp13data.khhm.str.strip().str.len()==8) & (self.rp_cp13data.khsy.str.strip().str.startswith('^H'))))
                                    & (self.rp_cp13data.rq == self.rq1)]['je'].sum()/10000).round(2)

        #本月
        jingxiaomonth_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                    & ((self.rp_cp13data.khhm.str.strip().str.len()==6)
                                    | ((self.rp_cp13data.khhm.str.strip().str.len()==8) & (self.rp_cp13data.khsy.str.strip().str.startswith('^H'))))
                                    & (self.rp_cp13data.rq <= self.rq1) & (self.rp_cp13data.rq >= self.rq2)]['je'].sum()/10000).round(2)

        #上年同月
        jingxiaolastmonth_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                    & ((self.rp_cp13data.khhm.str.strip().str.len()==6)
                                    | ((self.rp_cp13data.khhm.str.strip().str.len()==8) & (self.rp_cp13data.khsy.str.strip().str.startswith('^H'))))
                                    & (self.rp_cp13data.rq <= self.rq7) & (self.rp_cp13data.rq >= self.rq6)]['je'].sum()/10000).round(2)

        #本季
        jingxiaoquarter_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                    & ((self.rp_cp13data.khhm.str.strip().str.len()==6)
                                    | ((self.rp_cp13data.khhm.str.strip().str.len()==8) & (self.rp_cp13data.khsy.str.strip().str.startswith('^H'))))
                                    & (self.rp_cp13data.rq <= self.rq11) & (self.rp_cp13data.rq >= self.rq10)]['je'].sum()/10000).round(2)

        #上年同季
        jingxiaolastquarter_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                    & ((self.rp_cp13data.khhm.str.strip().str.len()==6)
                                    | ((self.rp_cp13data.khhm.str.strip().str.len()==8) & (self.rp_cp13data.khsy.str.strip().str.startswith('^H'))))
                                    & (self.rp_cp13data.rq <= self.rq9) & (self.rp_cp13data.rq >= self.rq8)]['je'].sum()/10000).round(2)

        #本年
        jingxiaoyear_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                    & ((self.rp_cp13data.khhm.str.strip().str.len()==6)
                                    | ((self.rp_cp13data.khhm.str.strip().str.len()==8) & (self.rp_cp13data.khsy.str.strip().str.startswith('^H'))))
                                    & (self.rp_cp13data.rq <= self.rq1) & (self.rp_cp13data.rq >= self.rq3)]['je'].sum()/10000).round(2)

        #上年同期
        jingxiaolastyear_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                    & ((self.rp_cp13data.khhm.str.strip().str.len()==6)
                                    | ((self.rp_cp13data.khhm.str.strip().str.len()==8) & (self.rp_cp13data.khsy.str.strip().str.startswith('^H'))))
                                    & (self.rp_cp13data.rq <= self.rq5) & (self.rp_cp13data.rq >= self.rq4)]['je'].sum()/10000).round(2)

        #把经销数据存放在字典中
        self.data_dict['经销'] = [jingxiaotoday_data,jingxiaomonth_data,jingxiaolastmonth_data,jingxiaoquarter_data
            ,jingxiaolastquarter_data,jingxiaoyear_data,jingxiaolastyear_data,290000]


        # 2.战略
        #本日
        zhanluetoday_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                               & (self.rp_cp13data.khhm.str.strip().str.len()==8)
                                               & (self.rp_cp13data.khsy.str.strip().str.startswith('H'))
                                               & (self.rp_cp13data.rq == self.rq1)]['je'].sum()/10000 \
                             + self.rp_cp23data[(self.rp_cp23data.db=='mon') & (self.rp_cp23data.rq == self.rq1)]['je'].sum()/10000).round(2)

        #本月
        zhanluemonth_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                               & (self.rp_cp13data.khhm.str.strip().str.len()==8)
                                               & (self.rp_cp13data.khsy.str.strip().str.startswith('H'))
                                               & (self.rp_cp13data.rq <= self.rq1) & (self.rp_cp13data.rq >= self.rq2)]['je'].sum()/10000 \
                             + self.rp_cp23data[(self.rp_cp23data.db=='mon') & (self.rp_cp23data.rq >= self.rq2) & (self.rp_cp23data.rq <= self.rq1)]['je'].sum()/10000).round(2)

        #上年同月
        zhanluelastmonth_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                               & (self.rp_cp13data.khhm.str.strip().str.len()==8)
                                               & (self.rp_cp13data.khsy.str.strip().str.startswith('H'))
                                               & (self.rp_cp13data.rq >= self.rq6) & (self.rp_cp13data.rq <= self.rq7)]['je'].sum()/10000 \
                             + self.rp_cp23data[(self.rp_cp23data.db=='mon') & (self.rp_cp23data.rq >= self.rq6) & (self.rp_cp23data.rq <= self.rq7)]['je'].sum()/10000).round(2)

        #本季
        zhanluequarter_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                               & (self.rp_cp13data.khhm.str.strip().str.len()==8)
                                               & (self.rp_cp13data.khsy.str.strip().str.startswith('H'))
                                               & (self.rp_cp13data.rq >= self.rq10) & (self.rp_cp13data.rq <= self.rq11)]['je'].sum()/10000 \
                             + self.rp_cp23data[(self.rp_cp23data.db=='mon') & (self.rp_cp23data.rq >= self.rq10) & (self.rp_cp23data.rq <= self.rq11)]['je'].sum()/10000).round(2)

        #上年同季
        zhanluelastquarter_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                               & (self.rp_cp13data.khhm.str.strip().str.len()==8)
                                               & (self.rp_cp13data.khsy.str.strip().str.startswith('H'))
                                               & (self.rp_cp13data.rq >= self.rq8) & (self.rp_cp13data.rq <= self.rq9)]['je'].sum()/10000 \
                             + self.rp_cp23data[(self.rp_cp23data.db=='mon') & (self.rp_cp23data.rq >= self.rq8) & (self.rp_cp23data.rq <= self.rq9)]['je'].sum()/10000).round(2)

        #本年
        zhanlueyear_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                               & (self.rp_cp13data.khhm.str.strip().str.len()==8)
                                               & (self.rp_cp13data.khsy.str.strip().str.startswith('H'))
                                               & (self.rp_cp13data.rq >= self.rq3) & (self.rp_cp13data.rq <= self.rq1)]['je'].sum()/10000 \
                             + self.rp_cp23data[(self.rp_cp23data.db=='mon') & (self.rp_cp23data.rq >= self.rq3) & (self.rp_cp23data.rq <= self.rq1)]['je'].sum()/10000).round(2)

        #上年同期
        zhanluelastyear_data =  (self.rp_cp13data[(self.rp_cp13data.db.isin(['smn','mon']))
                                               & (self.rp_cp13data.khhm.str.strip().str.len()==8)
                                               & (self.rp_cp13data.khsy.str.strip().str.startswith('H'))
                                               & (self.rp_cp13data.rq >= self.rq4) & (self.rp_cp13data.rq <= self.rq5)]['je'].sum()/10000 \
                             + self.rp_cp23data[(self.rp_cp23data.db=='mon') & (self.rp_cp23data.rq >= self.rq4) & (self.rp_cp23data.rq <= self.rq5)]['je'].sum()/10000).round(2)

        #把战略数据存放在字典中
        self.data_dict['战略'] = [zhanluetoday_data,zhanluemonth_data,zhanluelastmonth_data,zhanluequarter_data
            ,zhanluelastquarter_data,zhanlueyear_data,zhanluelastyear_data,330000]

        # 3.蒙娜丽莎合计（经销+战略）
        self.data_dict['蒙娜丽莎合计'] = [i + j for i , j in zip(self.data_dict['经销'] , self.data_dict['战略'])]

        # 4.蒙创
        #本日
        qdtoday_data = (self.rp_cp13data[(self.rp_cp13data.pb == 'QD') & (self.rp_cp13data.rq == self.rq1)]['je'].sum()/10000).round(2)

        #本月
        qdmonth_data = (self.rp_cp13data[(self.rp_cp13data.pb == 'QD') & (self.rp_cp13data.rq >= self.rq2)
                                         & (self.rp_cp13data.rq <= self.rq1)]['je'].sum()/10000).round(2)

        #上年同月
        qdlastmonth_data = (self.rp_cp13data[(self.rp_cp13data.pb == 'QD') & (self.rp_cp13data.rq >= self.rq6)
                                             & (self.rp_cp13data.rq <= self.rq7)]['je'].sum()/10000).round(2)

        #本季
        qdquarter_data = (self.rp_cp13data[(self.rp_cp13data.pb == 'QD') & (self.rp_cp13data.rq >= self.rq10)
                                           & (self.rp_cp13data.rq <= self.rq11)]['je'].sum()/10000).round(2)

        #上年同季
        qdlastquarter_data = (self.rp_cp13data[(self.rp_cp13data.pb == 'QD') & (self.rp_cp13data.rq >= self.rq8)
                                               & (self.rp_cp13data.rq <= self.rq9)]['je'].sum()/10000).round(2)

        #本年
        qdyear_data = (self.rp_cp13data[(self.rp_cp13data.pb == 'QD') & (self.rp_cp13data.rq >= self.rq3)
                                        & (self.rp_cp13data.rq <= self.rq1)]['je'].sum()/10000).round(2)

        #上年同期
        qdlastyear_data = (self.rp_cp13data[(self.rp_cp13data.pb == 'QD') & (self.rp_cp13data.rq >= self.rq4)
                                            & (self.rp_cp13data.rq <= self.rq5)]['je'].sum()/10000).round(2)

        # 把蒙创数据存放在字典中
        self.data_dict['蒙创'] = [qdtoday_data,qdmonth_data,qdlastmonth_data,qdquarter_data
            ,qdlastquarter_data,qdyear_data,qdlastyear_data,35000]

        # 5.绿屋-产品销售
        #本日
        vlxstoday_data = (self.rp_cp13data[((self.rp_cp13data.db == 'gn') | ((self.rp_cp13data.db == 'pb')
                                                           & ((self.rp_cp13data.khhm.str.strip().str.len() == 6) | (self.rp_cp13data.khhm.str.strip().str.len() == 8)))) \
                         & (self.rp_cp13data.rq == self.rq1)]['je'].sum()/10000).round(2)

        #本月
        vlxsmonth_data = (self.rp_cp13data[((self.rp_cp13data.db == 'gn') | ((self.rp_cp13data.db == 'pb')
                                                           & ((self.rp_cp13data.khhm.str.strip().str.len() == 6) | (self.rp_cp13data.khhm.str.strip().str.len() == 8)))) \
                         & (self.rp_cp13data.rq >= self.rq2) & (self.rp_cp13data.rq <= self.rq1)]['je'].sum()/10000).round(2)

        #上年同月
        vlxslastmonth_data = (self.rp_cp13data[((self.rp_cp13data.db == 'gn') | ((self.rp_cp13data.db == 'pb')
                                                           & ((self.rp_cp13data.khhm.str.strip().str.len() == 6) | (self.rp_cp13data.khhm.str.strip().str.len() == 8)))) \
                         & (self.rp_cp13data.rq >= self.rq6) & (self.rp_cp13data.rq <= self.rq7)]['je'].sum()/10000).round(2)

        #本季
        vlxsquarter_data = (self.rp_cp13data[((self.rp_cp13data.db == 'gn') | ((self.rp_cp13data.db == 'pb')
                                                           & ((self.rp_cp13data.khhm.str.strip().str.len() == 6) | (self.rp_cp13data.khhm.str.strip().str.len() == 8)))) \
                         & (self.rp_cp13data.rq >= self.rq10) & (self.rp_cp13data.rq <= self.rq11)]['je'].sum()/10000).round(2)

        #上年同季
        vlxslastquarter_data = (self.rp_cp13data[((self.rp_cp13data.db == 'gn') | ((self.rp_cp13data.db == 'pb')
                                                           & ((self.rp_cp13data.khhm.str.strip().str.len() == 6) | (self.rp_cp13data.khhm.str.strip().str.len() == 8)))) \
                         & (self.rp_cp13data.rq >= self.rq8) & (self.rp_cp13data.rq <= self.rq9)]['je'].sum()/10000).round(2)

        #本年
        vlxsyear_data = (self.rp_cp13data[((self.rp_cp13data.db == 'gn') | ((self.rp_cp13data.db == 'pb')
                                                           & ((self.rp_cp13data.khhm.str.strip().str.len() == 6) | (self.rp_cp13data.khhm.str.strip().str.len() == 8)))) \
                         & (self.rp_cp13data.rq >= self.rq3) & (self.rp_cp13data.rq <= self.rq1)]['je'].sum()/10000).round(2)

        #上年同期
        vlxslastyear_data = (self.rp_cp13data[((self.rp_cp13data.db == 'gn') | ((self.rp_cp13data.db == 'pb')
                                                           & ((self.rp_cp13data.khhm.str.strip().str.len() == 6) | (self.rp_cp13data.khhm.str.strip().str.len() == 8)))) \
                         & (self.rp_cp13data.rq >= self.rq4) & (self.rp_cp13data.rq <= self.rq5)]['je'].sum()/10000).round(2)

        self.data_dict['产品销售'] = [vlxstoday_data,vlxsmonth_data,vlxslastmonth_data
            ,vlxsquarter_data,vlxslastquarter_data,vlxsyear_data,vlxslastyear_data,40300]

        # 6.工程安装
        #本日
        lvgctoday_data = self.gn_cp17_g[self.gn_cp17_g.rq == self.rq1]['je'].sum()/10000

        #本月
        lvgcmonth_data = self.gn_cp17_g[(self.gn_cp17_g.rq >= self.rq2) & (self.gn_cp17_g.rq <= self.rq1)]['je'].sum()/10000

        #上年同月
        lvgclastmonth_data = self.gn_cp17_g[(self.gn_cp17_g.rq >= self.rq6) & (self.gn_cp17_g.rq <= self.rq7)]['je'].sum()/10000

        #本季
        lvgcquarter_data = self.gn_cp17_g[(self.gn_cp17_g.rq >= self.rq10) & (self.gn_cp17_g.rq <= self.rq11)]['je'].sum()/10000

        #上年同季
        lvgclastquarter_data = self.gn_cp17_g[(self.gn_cp17_g.rq >= self.rq8) & (self.gn_cp17_g.rq <= self.rq9)]['je'].sum()/10000

        #本年
        lvgcyear_data = self.gn_cp17_g[(self.gn_cp17_g.rq >= self.rq3) & (self.gn_cp17_g.rq <= self.rq1)]['je'].sum()/10000

        #上年同期
        lvgclastyear_data = self.gn_cp17_g[(self.gn_cp17_g.rq >= self.rq4) & (self.gn_cp17_g.rq <= self.rq5)]['je'].sum()/10000

        self.data_dict['工程安装'] = [lvgctoday_data,lvgcmonth_data,lvgclastmonth_data
            ,lvgcquarter_data,lvgclastquarter_data,lvgcyear_data,lvgclastyear_data,10000]

        # 7.绿屋-合计
        self.data_dict['绿屋合计'] = [i + j for i , j in zip(self.data_dict['产品销售'],self.data_dict['工程安装'])]

        # 8.贸易
        #本日
        pbtoday_data = (self.rp_cp13data[(self.rp_cp13data.pb == '出口') & (self.rp_cp13data.rq == self.rq1)]['je'].sum()/10000).round(2)

        #本月
        pbmonth_data = (self.rp_cp13data[(self.rp_cp13data.pb == '出口') & (self.rp_cp13data.rq >= self.rq2)
                                         & (self.rp_cp13data.rq <= self.rq1)]['je'].sum()/10000).round(2)

        #上年同月
        pblastmonth_data = (self.rp_cp13data[(self.rp_cp13data.pb == '出口') & (self.rp_cp13data.rq >= self.rq6)
                                         & (self.rp_cp13data.rq <= self.rq7)]['je'].sum()/10000).round(2)

        #本季
        pbquarter_data = (self.rp_cp13data[(self.rp_cp13data.pb == '出口') & (self.rp_cp13data.rq >= self.rq10)
                                         & (self.rp_cp13data.rq <= self.rq11)]['je'].sum()/10000).round(2)

        #上年同季
        pblastquarter_data = (self.rp_cp13data[(self.rp_cp13data.pb == '出口') & (self.rp_cp13data.rq >= self.rq8)
                                         & (self.rp_cp13data.rq <= self.rq9)]['je'].sum()/10000).round(2)

        #本年
        pbyear_data = (self.rp_cp13data[(self.rp_cp13data.pb == '出口') & (self.rp_cp13data.rq >= self.rq3)
                                         & (self.rp_cp13data.rq <= self.rq1)]['je'].sum()/10000).round(2)

        #上年同期
        pblastyear_data = (self.rp_cp13data[(self.rp_cp13data.pb == '出口') & (self.rp_cp13data.rq >= self.rq4)
                                         & (self.rp_cp13data.rq <= self.rq5)]['je'].sum()/10000).round(2)

        self.data_dict['贸易'] = [pbtoday_data,pbmonth_data,pblastmonth_data
            ,pbquarter_data,pblastquarter_data,pbyear_data,pblastyear_data,10000]

        #总计
        self.data_dict['总计'] = np.array(self.data_dict['蒙娜丽莎合计']) + np.array(self.data_dict['蒙创']) \
                               + np.array(self.data_dict['绿屋合计']) + np.array(self.data_dict['贸易'])


        #数据整合
        self.data_dict = pd.DataFrame(self.data_dict)
        #计算同期增幅
        self.data_dict.loc[8] = self.data_dict.apply(lambda x:0 if x.loc[5] == 0 else ((x.loc[5] - x.loc[6]) / x.loc[5]).round(4))

        #计算任务完成率
        self.data_dict.loc[9] = self.data_dict.apply(lambda x: 0 if x.loc[7] == 0 else (x.loc[5] / x.loc[7]).round(4))

        #设置列索引
        self.data_dict.index = ['本日开单','本月开单','上年同月','本季开单','上年同季','本年开单','上年同期','任务额','同期增幅','任务完成']

        return self.data_dict.T

