import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from decorator import log_information

class Send_Mail:

    def __init__(self,to_addrs):

        # 配置 授权码 端口号 等
        self.qqCode = '****'
        self.smtp_server = 'smtp.qq.com'
        self.from_addr = '111111@qq.com'
        self.to_addrs = to_addrs        #收件人email
        self.smtp_port = 465

    @log_information
    def Sendmail(self):
        '''发送邮件'''

        #配置服务器
        stmp = smtplib.SMTP_SSL(self.smtp_server,self.smtp_port)
        stmp.login(self.from_addr,self.qqCode)

        message = MIMEMultipart('related')
        message['Subject'] = '【蒙娜丽莎】【MCS高层报表】【销售开单】'
        message['From'] = self.from_addr
        message['To'] = self.to_addrs
        content = MIMEText('<html><body><img src="cid:imageid" alt="imageid"></body></html>','html','utf-8')
        message.attach(content)

        img_data = open(r'.\execl_file\xiaoshoukaidan.png','rb').read()

        img = MIMEImage(img_data)
        img.add_header('Content-ID','imageid')
        message.attach(img)

        try:
            stmp.sendmail(self.from_addr,self.to_addrs,message.as_string())
            print('发送成功')
            stmp.quit()
        except:
            print('发送失败')
