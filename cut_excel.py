from win32com.client import DispatchEx
from PIL import ImageGrab
from decorator import log_information
import pythoncom

class Cut_Excel:

    def __init__(self):

        #保存图片名
        self.img_name = 'xiaoshoukaidan'
        #文件夹的相对路径
        self.file_path = r'.\execl_file'

    @log_information
    def save_png(self):
        '''截取excel，并保存图片'''

        #开启多线程
        pythoncom.CoInitialize()
        #声明一个excel对象
        excel = DispatchEx('excel.application')

        #不显示excel
        excel.visible = False
        #关闭系统警告
        excel.DisplayAlerts = 0

        #excel文件的绝对路径
        excel_path = r'F:\pythonProject\execl_file\jingshoukaidan.xlsx'
        #打开excel文件
        workbook = excel.workbooks.Open(excel_path)
        #定位到“销售开单”sheet
        worksheet = workbook.worksheets['销售开单']
        #根据有内容区域进行复制图片
        worksheet.UsedRange.CopyPicture()
        #粘贴
        worksheet.Paste()

        #重命名图片
        excel.Selection.ShapeRange.Name = self.img_name
        #选择图片
        worksheet.Shapes(self.img_name).Copy()
        #获取剪贴板的图片数据
        img = ImageGrab.grabclipboard()

        #保存图片
        img_path = self.file_path + r'\{}.png'.format(self.img_name)
        img.save(img_path)

        #关闭excel，不保存
        workbook.Close(False)
        #退出excel
        excel.Quit()

        #关闭多线程
        pythoncom.CoUninitialize()


