# -*- coding=utf-8 -*-
import requests
import io
import sys
import json
import re
import os
import uuid
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from connmongo import MongoConn
from pymongo.bulk import BulkWriteError
fron pymongo.errors import DuplicateKeyError

# 添加全文索引
index_url = "http://10.9.70.92:8185/ssearch/addIndex"

def add_index(rst):
    if rst:
        for r in rst:
            cont = ''.join(r['content'])
            dt = re.sub('[-:\s]','',r['createTime'])
            param = {'title':r['title'],'contents':cont,'docPath':r['url'],'category':r['category'],'authority':'open','datetime':dt,'uid':r['url']}
            try:
                res = requests.post(url=index_url,data=json.dumps(param),headers={'Content-Type':'application/json'})
            except Exception as e:
                print(e)


# elasticsearch 实例
es = Elasticsearch(["10.9.60.12:9200"],maxsize=25)

def in_es(rst):
    if rst:
        for r in rst:
            res = r.copy()
            cont = '!@#'.join(r['content'])
            res['content'] = cont
            if res.get('_id'):
                del res['_id']
            try:
                es.index(body=res,doc_type="news",index=r['category'],id=r['title'])
            except Exception as e:
                print(e)

# 获取已存在url
def get_exists_urls():
    my_conn = MongoConn()
    exists_urls_rst = list(my_conn.db['getnews'].find({'source':'投资界'},{'url':1,'_id':0}))
    exists_urls0 = [i['url'] for i in exists_urls_rst]
    return exists_urls0


# 类别信息
catinfos = {
    'VC/PE':'http://pe.pedaily.cn/vcpe/',
    '天使':'http://pe.pedaily.cn/angel-investment/',
    '新三板':'http://pe.pedaily.cn/new-third-board/',
    '产业':'http://news.pedaily.cn/',
    '滚动新闻':'http://www.pedaily.cn/all/',
    '人物':'http://people.pedaily.cn/'
}

# 类别映射信息
newcats = {
    'VC/PE':'vc_pe',
    '天使':'angel',
    '产业':'industry_news',
    '人物':'characters'
}

# ['industry_news','vc_pe','angel','entrepreneur','big_companies','characters','finance_realestate']

def in_soup(url):
    html = requests.get(url)
    html.encoding = 'utf-8'
    htmltext = html.text
    souprst = BeautifulSoup(htmltext,features='lxml')
    return  souprst

# 获取最新文章url
def get_urls(cat,n):
    base_url = catinfos[cat]
    urls = [base_url + "{}/".format(i) for i in range(1,n+1)]
    frst = {}
    result = []
    for url in urls:
        soup = in_soup(url)
        rst = soup.find(id='newslist-all')
        inrst = rst.find_all("li")
        for r in inrst:
            try:
                titleimg0 = r.find(attrs={'class':'img'})
                titleimgurl = titleimg0.a.img['data-src']
            except AttributeError:
                titleimgurl = ""
            titleImg = str(uuid.uuid1())
            rst2 = r.find('h3')
            href = rst2.a['href']
            tupres = (titleImg,titleimgurl,href)
            if tupres not in result:
                result.append(tupres)
    frst['cat'] = newcats[cat]
    frst['val'] = reversed(result)
    return frst

# 获取文章标题，发表时间，内容，标签（投资界文章详情)
def get_want(url, cat, imgpath):
    rst = {}
    souprst = in_soup(url[2])
    exists_url = get_exists_urls()
    if souprst and souprst.find(id='newstitle') and url[2] not in exists_url:
        print("正在爬取:",url[2])
        rst['url'] = url[2]
        rst['title'] = souprst.find(id='newstitle').string
        rst['titleImg'] = url[0]
        if url[1]:
            titlepic = requests.get(url[1], stream=True)
            with open(os.path.join(imgpath, url[0]), 'wb') as f:
                for chunk in titlepic.iter_content(128):
                    f.write(chunk)
        rst['description'] = souprst.find(attrs={'class':'subject'}).string
        rst['dateTime'] = souprst.find(attrs={'class':'date'}).string
        rst['author'] = souprst.find(attrs={'class':'info'}).find(attrs={'class':'box-l'}).get_text().split('·')[-1]
        cont = souprst.find(id='news-content')
        raw_cont = cont.find_all('p')
        rst_cont = []
        for item in raw_cont:
            if item.find("img"):
                src = item.find('img')
                if src['src'].startswith('//'):
                    picurl = "http:" + src['src']
                elif not src['src'].startswith('data:image'):
                    picurl = src['src']
                else:
                    picurl = None
                if picurl:
                    name = str(uuid.uuid1())
                    path = imgpath
                    bpic = requests.get(picurl, stream=True)
                    with open(os.path.join(path, name ), 'wb') as f:
                        for chunk in bpic.iter_content(128):
                            f.write(chunk)
                    piccont = ''.join([str(i) for i in item.contents]).replace(src['src'], '###path' + name )
                    rst_cont.append(piccont)
                else:
                    rst_cont.append(''.join([str(i) for i in item.contents]))
            else:
                rst_cont.append(''.join([str(i) for i in item.contents]))
        rst['content'] = [i for i in rst_cont if i.strip()]
        rst['createTime'] = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
        rst['timestamp'] = int(datetime.now().timestamp()*1000)
        rst['category'] = cat
        rst['source'] = '投资界'
        raw_tag = souprst.find(attrs={'class':'box-l tag'}).find_all('a')
        rst_tag = []
        for tag in raw_tag:
            rst_tag.append(tag.string)
        rst['tag'] = ','.join(rst_tag)
    return rst

# 获取文章标题，发表时间，内容，标签（新芽文章详情)
def get_want2(url,cat,imgpath):
    rst = {}
    souprst = in_soup(url[2])
    exists_urls = get_exists_urls()
    if souprst and souprst.find(id='title') and url[2] not in exists_urls:
        print("正在爬取:",url[2])
        rst['url'] = url[2]
        rst['titleImg'] = url[0]
        if url[1]:
            titlepic = requests.get(url[1], stream=True)
            with open(os.path.join(imgpath, url[0]), 'wb') as f:
                for chunk in titlepic.iter_content(128):
                    f.write(chunk)
        rst['title'] = souprst.find(id='title').string
        rst['description'] = souprst.find(attrs={'class':'subject'}).string
        rst['author'] = souprst.find(attrs={'class':'author'}).string
        rst['dateTime'] = souprst.find(attrs={'class':'date'}).string
        cont = souprst.find(id='news-content')
        raw_cont = cont.find_all('p')
        rst_cont = []
        for item in raw_cont:
            if item.find("img"):
                src = item.find('img')
                if src['src'].startswith('//'):
                    picurl = "http:" + src['src']
                elif src['src'].startswith('data:image'):
                    picurl = src['src']
                else:
                    picurl = None
                if picurl:
                    name = str(uuid.uuid1())
                    path = imgpath
                    bpic = requests.get(picurl, stream=True)
                    with open(os.path.join(path, name ), 'wb') as f:
                        for chunk in bpic.iter_content(128):
                            f.write(chunk)
                    piccont = ''.join([str(i) for i in item.contents]).replace(src['src'], '###path' + name )
                    rst_cont.append(piccont)
                else:
                    rst_cont.append(''.join([str(i) for i in item.contents]))
            else:
                rst_cont.append(''.join([str(i) for i in item.contents]))
        rst['content'] = [i for i in rst_cont if i.strip()]
        rst['createTime'] = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
        rst['timestamp'] = int(datetime.now().timestamp()*1000)
        rst['category'] = cat
        rst['source'] = '投资界'
        raw_tag = souprst.find(attrs={'class':'news-tags'}).find_all('a')
        rst_tag = []
        for tag in raw_tag:
            rst_tag.append(tag.string)
        rst['tag'] = ','.join(rst_tag)
    return rst

# 插入mongo数据库
def in_db(allrst):
    my_conn = None
    success_item = 0
    assert allrst and isinstance(allrst[0],dict),'参数须为字典组成的列表'
    try:
        my_conn = MongoConn()
    except Exception:
        print("连接mongo失败")
    if len(allrst) > 0:
        rst1_add = [item for item in allrst if item]
        print("正在存入数据库，新增%s条" % len(rst1_add))
        for it in rst1_add:
            try:
                my_conn.db['getnews'].insert_one(it)
                success_item += 1
            except TypeError:
                print("参数为空值，插入失败")
            except DuplicateKeyError:
                pass
            except Exception as e:
                print("异常",str(e))
    return  success_item

def final(cats,n,imgpath):
    if isinstance(cats,str):
        assert cats in catinfos.keys(),'类别不在范围内'
    allrst = []
    if isinstance(cats,list) and len(cats) > 0:
        urls = [get_urls(cat,n) for cat in cats]
    elif isinstance(cats,str) and cats:
        urls = list(get_urls(cats,n))
    else:
        return -1
    for item in urls:
        cat = item['cat']
        for url in item['val']:
            if url[2].find("newseed") > 0:
                rst = get_want2(url, cat,imgpath)
                if rst:
                    allrst.append(rst)
            else:
                rst = get_want(url,cat,imgpath)
                if rst:
                    allrst.append(rst)
    res = 0
    if allrst:
        in_es(allrst)
        res = in_db(allrst)
    return res

def touzijie():
    res = final(['VC/PE','天使','产业','人物'],1,"/data0/getnews/images")
    return res

if __name__ == '__main__':
    res = touzijie()
    print(res)
    # res = get_want('http://news.pedaily.cn/201808/435060.shtml','测试',"C:/kyy/项目/get_finance_news/images")
    # print(res)
    # res = get_urls('人物',2)
    # print(res)
