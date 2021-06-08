import pandas as pd
import os
from data_deal import Data_deal
from decorator import log_information

class Format_Adjust:

    def __init__(self,data):
        '''data为data_deal处理后的dataframe'''

        #判断是否存在excel_file文件夹，不存在，则创建一个名为excel_file文件夹
        if not os.path.exists(r'.\execl_file'):
            os.mkdir(r'.\execl_file')

        #取出data_deal中处理好的数据
        # data_deal = Data_deal()
        # data_deal.data_extract()
        # self.data = data_deal.data_handle()

        self.data = data

    @log_information
    def fmt_adjust(self):
        '''
        调整输出excel样式
        :return: execl
        '''

        #保存到excel文件
        writer = pd.ExcelWriter(r'.\execl_file\jingshoukaidan.xlsx')
        workbook = writer.book

        #设置样式
        percent_fmt = workbook.add_format({'num_format': '0.00%'})
        border_format = workbook.add_format({'border': 1})
        note_fmt = workbook.add_format({'bold': True
                                           ,'font_size':16
                                           , 'font_name': u'微软雅黑'
                                           , 'font_color': 'red'
                                           , 'align': 'center'
                                           , 'valign': 'vcenter'})
        note_fmt1 = workbook.add_format({'bold': True
                                            ,'font_size':10
                                            , 'font_name': u'微软雅黑'
                                            , 'align': 'center'
                                            , 'valign': 'vcenter'})

        date_fmt1 = workbook.add_format({'bold': True
                                            ,'font_size': 10
                                            ,'font_name': u'微软雅黑'
                                            ,'num_format': 'yyyy-mm-dd'
                                            ,'bg_color': '#9FC3D1'
                                            ,'valign': 'vcenter'
                                            , 'align': 'center'})
        percent_fmt1 = workbook.add_format({'bg_color': 'red'
                                                ,'num_format': '0.00%'})
        percent_fmt2 = workbook.add_format({'bg_color': 'green'
                                                ,'num_format': '0.00%'})

        #写入execl文件
        self.data.to_excel(writer
                      ,sheet_name=u'销售开单'
                      ,encoding='utf-8'
                      ,header=False
                      ,index=False
                      ,startcol=2
                      ,startrow=2)
        worksheet = writer.sheets[u'销售开单']

        for col_num,value in zip([i for i in range(2,len(self.data.columns)+2)],self.data.columns.values):
            worksheet.write(1,col_num,value,date_fmt1)

        #设置excel中的index，合并居中单元格等
        worksheet.merge_range('A1:L1',u'销售开单报表',note_fmt)
        worksheet.merge_range('A3:A5',u'蒙娜丽莎',note_fmt1)
        worksheet.merge_range('A6:B6',u'蒙创',note_fmt1)
        worksheet.merge_range('A7:A9',u'绿屋',note_fmt1)
        worksheet.merge_range('A10:B10',u'贸易',note_fmt1)
        worksheet.merge_range('A11:B11',u'总计',note_fmt1)
        worksheet.merge_range('A2:B2',u'单位:万元',note_fmt1)
        worksheet.write('B3:B3',u'经销',note_fmt1)
        worksheet.write('B4:B4',u'战略',note_fmt1)
        worksheet.write('B5:B5',u'合计',note_fmt1)
        worksheet.write('B7:B7',u'产品销售',note_fmt1)
        worksheet.write('B8:B8',u'工程安装',note_fmt1)
        worksheet.write('B9:B9',u'合计',note_fmt1)

        #增加边框
        worksheet.conditional_format('A1:L11', {'type': 'blanks', 'format': border_format})
        worksheet.conditional_format('A1:L11', {'type': 'no_blanks', 'format': border_format})

        #设置百分比
        worksheet.conditional_format('L3:L11'
                                     ,{'type':'no_blanks'
                                         ,'format':percent_fmt})

        #同比增幅：>0显示红色，<=0显示为绿色
        worksheet.conditional_format('K3:K11'
                                     ,{'type':'cell'
                                         ,'criteria':'>'
                                         ,'value':0
                                         ,'format':percent_fmt1})

        worksheet.conditional_format('K3:K11'
                                     ,{'type':'cell'
                                         ,'criteria':'<='
                                         ,'value':0
                                         ,'format':percent_fmt2})


        writer.save()



