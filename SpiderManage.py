# 日志
# 调用spider（包含spider运行状态）
# 图片相对路径
# 发送邮件
import LogManage
from multiprocessing import Process
from huxiu import huxiu
from datetime import datetime
from qingke_new import qingke
from touzhong_new import touzhong
from touzijie_new import touzijie



def spider_run(website_name, spider):
    """
    各网站爬虫运行
    :return:
    """
    try:
        data_num = spider()
        LogManage.MyLog.info(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")+ "----" + website_name + "入库数量:" +  str(data_num))
    except Exception as e:
        LogManage.MyLog.error(str(website_name) + ": " + str(e))


# 爬虫列表
spider_list = [{'qingke': qingke}, {'huxiu': huxiu}, {'touzhong': touzhong}, {'touzijie':touzijie}]
process_list = []

# 遍历列表运行爬虫
for i in spider_list:
    key = list(i.keys())[0]
    value = list(i.values())[0]
    p = Process(target=spider_run, name=key, args=(key, value))
    process_list.append(p)

# 开启进程
for i in process_list:
    i.start()

# 阻塞进程
for i in process_list:
    i.join()

