import requests
from lxml import etree
import json
import pymongo
import queue
import uuid
from datetime import datetime
import LogManage
from elasticsearch import Elasticsearch
import time


class News(object):
    def __init__(self):
        self.client = pymongo.MongoClient(host='10.9.60.12', port=27017)
        self.db = self.client.finance_news
        self.collection = self.db.getnews
        self.es = Elasticsearch(["10.9.60.12:9200"], maxsize=25)
        self.data_num = 0
        self.base_list_url = 'https://www.huxiu.com/channel/%s.html'
        self.base_details_url = 'https://www.huxiu.com/channel/ajaxGetMore'
        self.base_url = 'https://www.huxiu.com'
        self.detail_url = queue.Queue()
        self.url_list = [
            ['102', "finance_realestate"],  # 金融地产
            ['2',   "entrepreneur"],  # 创业动态
        ]
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
        }

    def create_uuid(self):
        return str(uuid.uuid4())

    def detail_page(self, url):
        """
        获取详情页数据
        :param url:
        :return:
        """
        response = requests.get(url, headers=self.headers)
        return response.content

    def fir_page(self, url_path):
        """
        获取各分类首页详情页url
        :param url_path:
        :return:
        """
        url = self.base_list_url % url_path
        response = requests.get(url, headers=self.headers).content
        return response

    def nofir_page(self, url_path, page):
        """
        获取各分类后续加载更多页面
        :param url_path:
        :param page:
        :return:
        """
        url = self.base_details_url
        response = json.loads(
            requests.post(url, data={'page': page, 'catId': url_path},
                          headers=self.headers).content.decode())['data']['data']
        return response

    def generate_url(self, response, catetory):
        """
        提取列表页的url
        :param response:
        :param catetory:
        :return:
        """
        html = etree.HTML(response)
        list_div = html.xpath("//*[@class='mod-b mod-art clearfix']")
        for each_div in list_div[::-1]:
            detail_link = each_div.xpath('./div/h2/a/@href')[0]
            if not self.collection.find_one({'url': self.base_url + detail_link}):
                description = each_div.xpath("./div[@class='mob-ctt channel-list-yh']/div[@class='mob-sub']/text()")[0]
                self.detail_url.put_nowait(self.base_url + detail_link + ',' + catetory + ',' + description)

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
        dict_data['title'] = response_html.xpath('//*[@class="t-h1"]/text()')[0].strip()
        dict_data['author'] = response_html.xpath('//*[@class="article-author"]/span/a/text()')[0].strip()
        dict_data['dateTime'] = response_html.xpath('//*[@class="article-time pull-left"]/text()')[0].strip()
        dict_data['tag'] = ','.join(response_html.xpath('//*[@class="transition"]/a/li/text()'))
        img_src = response_html.xpath('//*[@class="article-img-box"]/img/@src')[0].strip()
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
                if len(content_per.xpath('./strong')) != 0:
                    strong_msg = etree.tostring(content_per.xpath('./strong')[0], encoding="utf-8", method='html',
                                                pretty_print=True).decode()
                    dict_data['content'].append(strong_msg)
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
                    img_msg = img_msg.replace('\n', '').replace(img_link, '###' + img_name)
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
        dict_data['content'] = '!@#'.join(dict_data['content'])
        self.es.index(body=dict_data, index=dict_data['category'], doc_type="news", id=dict_data['title'])
        self.collection.insert_one(dict_data)
        print(dict_data)
        # ['industry_news', 'vc_pe', 'angel', 'entrepreneur', 'big_companies', 'characters', 'finance_realestate']
        self.data_num += 1

    def __del__(self):
        self.client.close()

    def detail_link_spider(self):
        """
        汇总详情页url
        :return:
        """
        for url_path in self.url_list:
            try:
                fir_response = self.fir_page(url_path[0])
                self.generate_url(fir_response, url_path[1])

            except Exception as f:
                LogManage.MyLog.error(str('huxiu111') + ": " + str(f))
                continue

    def detail_link_spider_more(self, page):
        """
        汇总详情页url
        :return:
        """
        for url_path in self.url_list:
            try:
                fir_response = self.fir_page(url_path[0])
                self.generate_url(fir_response, url_path[1])
                for i in range(2, int(page)+1):
                    nofir_response = self.nofir_page(url_path[0], i)
                    self.generate_url(nofir_response, url_path[1])
            except Exception as f:
                LogManage.MyLog.error(str('huxiu111') + ": " + str(f))
                continue

    def detail_data_spider(self):
        """
        详情页数据保存
        :return:
        """
        while True:
            try:
                if self.detail_url.qsize() == 0:
                    break
                detail_page_link = self.detail_url.get()
                detail_data = self.parse_data(detail_page_link)
                self.save_data(detail_data)
            except Exception as f:
                LogManage.MyLog.error(str('huxiu222') + ": " + str(f))
                continue

    def run(self, page=1):
        if page == 1:
            self.detail_link_spider()
        else:
            self.detail_link_spider_more(page)
        self.detail_data_spider()
        return self.data_num


def huxiu(page=1):
    qiushi = News()
    date_num = qiushi.run(page)
    return date_num

huxiu(page=10)
