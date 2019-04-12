import requests
from lxml import etree
import json
import pymongo
import uuid
from datetime import datetime
import LogManage
from elasticsearch import Elasticsearch


class News(object):
    def __init__(self):
        self.client = pymongo.MongoClient(host='10.9.60.12', port=27017)
        self.db = self.client.finance_news
        self.collection = self.db.getnews
        self.es = Elasticsearch(["10.9.60.12:9200"], maxsize=25)
        self.data_num = 0
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
        }
        self.base_url = "https://36kr.com/api/search-column/23?per_page=50&page=1"
        self.list_page = None

    def create_uuid(self):
        return str(uuid.uuid4())

    def requests_page(self, url):
        """
        获取页面数据
        :param url:
        :return:
        """
        response = requests.get(url, headers=self.headers)
        return response

    def generate_url(self, detail_page_msg):
        """
        将json数据做处理
        :param response:
        :param catetory:
        :return:
        """
        detail_page_msg['url'] = 'https://36kr.com/p/' + detail_page_msg['id'] + '.html'
        return detail_page_msg

    def parse_data(self, link):
        """
        提取详情页各数据
        :param link:
        :return:
        """
        link, catetory, description = link.split(',')
        response = self.detail_page(link)
        response_html = etree.HTML(response)
        dict_data = {}
        dict_data['description'] = description
        dict_data['title'] = response_html.xpath('//*[@class="t-h1"]/text()')[0]
        dict_data['author'] = response_html.xpath('//*[@class="article-author"]/span/a/text()')[0]
        dict_data['dateTime'] = response_html.xpath('//*[@class="article-time pull-left"]/text()')[0]
        dict_data['tag'] = ','.join(response_html.xpath('//*[@class="transition"]/a/li/text()'))
        img_src = response_html.xpath('//*[@class="article-img-box"]/img/@src')[0]
        img_response = requests.get(img_src).content
        img_name = self.create_uuid()
        with open('/data0/getnews/images/' + img_name, 'wb')as f:
            f.write(img_response)
        dict_data['titleImg'] = img_name
        dict_data['url'] = link
        dict_data['source'] = 'huxiu'
        dict_data['category'] = catetory
        dict_data['createTime'] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        dict_data['timestamp'] = int(datetime.timestamp(datetime.now()) * 1000)
        content = response_html.xpath('//*[@class="article-content-wrap"]/p')
        dict_data['content'] = []
        for content_per in content:
            try:
                if len(content_per.xpath('./text()')) != 0:
                    text_msg = ''.join(content_per.xpath('./text()')).replace('\n', '')
                    dict_data['content'].append(text_msg)
                if len(content_per.xpath('./img')) != 0:
                    img_link = content_per.xpath('./img/@src')[0]
                    img_response = requests.get(img_link).content
                    img_name = self.create_uuid()
                    with open('/data0/getnews/images/' + img_name, 'wb')as f:
                        f.write(img_response)
                    img_msg = etree.tostring(content_per.xpath('./img')[0], encoding="utf-8", method='html',
                                             pretty_print=True).decode()
                    img_msg = img_msg.replace('\n', '').replace(img_link, '$path:' + img_name + '.jpg')
                    dict_data['content'].append(img_msg)
            except:
                continue
        dict_data['content'] = [x for x in dict_data['content'] if x.strip()]
        return dict_data

    def save_data(self, dict_data):
        """
        数据入库
        :param dict_data:
        :return:
        """
        dict_data['content'] = ''.join(dict_data['content'])
        self.es.index(body=dict_data, index=dict_data['category'], doc_type="news")
        self.collection.insert_one(dict_data)
        print(dict_data)
        # ['industry_news', 'vc_pe', 'angel', 'entrepreneur', 'big_companies', 'characters', 'finance_realestate']
        self.data_num += 1

    def __del__(self):
        self.client.close()

    def list_page_parse(self):
        """
        列表页数据解析
        :return:
        """
        self.list_page = self.requests_page(self.base_url).json()['data']['items']

    def detail_data_spider(self, detail_page_msg):
        """
        详情页数据保存
        :return:
        """
        detail_page_msg = self.generate_url(detail_page_msg)
        dict_data = self.parse_data(detail_page_msg)
        self.save_data(dict_data)

    def run(self):
        self.list_page_parse()
        for detail_page_msg in self.list_page:
            self.detail_data_spider(detail_page_msg)


def bigcompany36kr():
    bigcompany = News()
    date_num = bigcompany.run()
    return date_num


