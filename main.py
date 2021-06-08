from data_deal import Data_deal
from format_adjust import Format_Adjust
from cut_excel import Cut_Excel
from send_mail import Send_Mail
from apscheduler.schedulers.blocking import BlockingScheduler
from connect_database import Conn_database
from logger import Logger

sched = BlockingScheduler()

logger__ = Logger().get_log()

@sched.scheduled_job('interval', minutes=5)
def main():
    try:
        #检查数据库连接是否正常
        Conn_database()

        # 数据抽取，数据处理，数据整合
        data_deal = Data_deal()
        data_deal.data_extract()
        xiaoshoukaidan_data = data_deal.data_handle()

        # 调整样式，保存excel文件
        fmt = Format_Adjust(xiaoshoukaidan_data)
        fmt.fmt_adjust()

        #截图
        ct = Cut_Excel()
        ct.save_png()

        #发邮件
        send_mail = Send_Mail(to_addrs='111111@qq.com')
        send_mail.Sendmail()
    except:

        #数据库连不上，写入日志
        logger__.error('数据库连不上，请检查数据库连接')


sched.start()



