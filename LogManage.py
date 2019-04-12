# coding=utf-8
import logging
import os
import time

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL, }

logger = logging.getLogger()
level = 'default'


def createFile(filename):
    path = filename[0:filename.rfind('/')]
    if not os.path.isdir(path):
        os.makedirs(path)
    if not os.path.isfile(filename):
        # 创建并打开一个新文件
        fd = open(filename, mode='w', encoding='utf-8')
        fd.close()


class MyLog:
    log_filename = '/data0/getnews/script/log/data.log'
    err_filename = '/data0/getnews/script/log/err.log'
    dateformat = '%Y-%m-%d %H:%M:%S'
    logger.setLevel(LEVELS.get(level, logging.NOTSET))
    createFile(log_filename)
    createFile(err_filename)
    # 注意文件内容写入时编码格式指定
    handler = logging.FileHandler(log_filename, encoding='utf-8')
    errhandler = logging.FileHandler(err_filename, encoding='utf-8')

    @staticmethod
    # 静态方法
    def debug(log_message):
        setHandler('debug')
        logger.debug("[DEBUG " + getCurrentTime() + "]" + log_message)
        removerhandler('debug')

    @staticmethod
    def info(log_message):
        setHandler('info')
        logger.info("[INFO " + getCurrentTime() + "]" + log_message)
        removerhandler('info')

    @staticmethod
    def warning(log_message):
        setHandler('warning')
        logger.warning("[WARNING " + getCurrentTime() + "]" + log_message)
        removerhandler('warning')

    @staticmethod
    def error(log_message):
        setHandler('error')
        logger.error("[ERROR " + getCurrentTime() + "]" + log_message)
        removerhandler('error')

    @staticmethod
    def critical(log_message):
        setHandler('critical')
        logger.critical("[CRITICAL " + getCurrentTime() + "]" + log_message)
        removerhandler('critical')


# logger可以看做是一个记录日志的人，对于记录的每个日志，他需要有一套规则，比如记录的格式（formatter），
# 等级（level）等等，这个规则就是handler。使用logger.addHandler(handler)添加多个规则，
# 就可以让一个logger记录多个日志。
def setHandler(level):
    if level == 'error':
        logger.addHandler(MyLog.errhandler)
    # handler=logging.FileHandler(log_filename)
    # 把logger添加上handler
    logger.addHandler(MyLog.handler)


def removerhandler(level):
    if level == 'error':
        logger.removeHandler(MyLog.errhandler)
    logger.removeHandler(MyLog.handler)


def getCurrentTime():
    return time.strftime(MyLog.dateformat, time.localtime(time.time()))


if __name__ == "__main__":
    MyLog.debug("This is debug message")
    MyLog.info("This is info message")
    MyLog.warning("This is warning message")
    MyLog.error("This is error message")
    MyLog.critical("This is critical message")
