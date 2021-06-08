import time
import functools
from logger import Logger
import traceback

logger_ = Logger().get_log()

def log_information(func):

    @functools.wraps(func)
    def inner(*args, **kwargs):
        # 获取开始时间
        starttime_ = time.localtime()
        starttime = time.strftime('%Y-%m-%d %H:%M:%S', starttime_)
        logger_.info('开始任务%s , 开始时间为%s' % (func, starttime))

        #判断func执行是否会出错，若出错，捕获error，输出到日志
        try:
            ret = func(*args, *kwargs)

        except Exception as e:
            logger_.error('任务 %s 发生错误 , 错误原因 %s , 错误信息 %s' % (func, e, traceback.format_exc()))
            ret = None

        # 获取结束时间
        endtime_ = time.localtime()
        endtime = time.strftime('%Y-%m-%d %H:%M:%S', endtime_)
        # 计算执行用时
        usedtime = time.mktime(endtime_) - time.mktime(starttime_)

        logger_.info('结束任务%s , 结束时间为%s , 执行时长为%f s' % (func, endtime, usedtime))

        return ret

    return inner


