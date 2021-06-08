import logging
import time,os

class Logger:

    def __init__(self):

        #创建一个log文件夹，用来存放log日志文件
        if not os.path.exists(r'.\logs'):
            os.mkdir(r'.\logs')

        #实例化一个logger对象
        self.logger = logging.getLogger('自动化推送报表')
        #设置最低严重级别为DEBUG
        self.logger.setLevel(logging.DEBUG)

        #获取当前日期
        self.nowdate = time.strftime('%Y-%m-%d',time.localtime(time.time()))

        #每次调用都清空一下handlers
        self.logger.handlers.clear()

        #创建一下handlers，写入logs文件
        log_name = r'.\logs\log_{}.log'.format(self.nowdate)
        fh = logging.FileHandler(log_name,encoding='utf-8')

        #再创建一下handlers，将日志输出在工作台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        #定义handle输出格式
        formatter = logging.Formatter('%(asctime)s--%(name)s--%(levelname)s--%(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        #给logger添加handler
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)


    def get_log(self):
        return self.logger