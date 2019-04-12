import requests
import LogManage
from lxml import etree
from datetime import datetime, timedelta
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch

headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
  'Referer': 'https://36kr.com/newsflashes',
  'Host': '36kr.com'
}

es = Elasticsearch( "http://10.9.60.12:9200" )

def kuaixu(page, ago):
  now = datetime.now()
  timestamp = ''.join(str(now.timestamp()).split('.'))
  params = {'b_id':999999, 'per_page': page, '_': timestamp}
  response = requests.get('https://36kr.com/api/newsflash', params=params)
  datas = response.json()
  datas = datas['data']['items']
  items = []
  # ago = datetime.strptime(ago, '%Y-%m-%d %H:%M:%S')
  ago = datetime.now() - timedelta(hours=ago)
  for data in datas:
    pus_str = data['published_at']
    pus = datetime.strptime(pus_str, '%Y-%m-%d %H:%M:%S')
    if pus < ago:
      break
    item = {}
    id = data['id']
    item['id'] = id
    item['titleImg'] = ''
    item['description'] = data['description']
    item['dateTime'] = pus_str
    item['createTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    item['content'] = ''
    item['timestamp'] = int(datetime.timestamp(datetime.now()) * 1000)
    item['title'] = data['title']
    item['url'] = data.get('news_url', '')
    item['author'] = data['user']['name']
    item['category'] = '快讯'
    item['source'] = ''
    try:
      res = es.get(index="news_flash", doc_type='news', id=id)
      if res:
        break
    except Exception as e:
      print( '重复数据', e, item)
      try:
        res = es.index(index="news_flash", doc_type='news', id=id, body=item)
        print('插入成功', res)
      except Exception as ce:
        print('插入失败', ce, item)
    items.append(item)
  LogManage.MyLog.info(datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S") + "-----------" + "从news36kr获取快讯数目:" + str(len(items)) + "条")
  return items

def news36kr():
  result = kuaixu(50, 1)

news36kr()



