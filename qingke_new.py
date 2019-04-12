# -*- coding=utf-8 -*-
import requests
import io,os
import sys
import json
import re
import uuid
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from connmongo import MongoConn
from pymongo.errors import DuplicateKeyError

# 添加全文索引
index_url = "http://10.9.70.92:8185/ssearch/addIndex"

def add_index(rst):
    if rst:
        for r in rst:
            if len(r) == 9:
                try:
                    cont = ''.join(r['content'])
                    dt = re.sub('[-:\s]','',r['createTime'])
                    param = {'title':r['title'],'contents':cont,'docPath':r['url'],'category':r['category'],'authority':'open','datetime':dt,'uid':r['url']}
                    res = requests.post(url=index_url,data=json.dumps(param),headers={'Content-Type':'application/json'})
                except Exception as e:
                    print(e)


# elasticsearch 实例
es = Elasticsearch(["10.9.60.12:9200"],maxsize=25)

def in_es(rst):
    if rst:
        for r in rst:
            if len(r) == 9:
                try:
                    res = r.copy()
                    cont = '!@#'.join(r['content'])
                    res['content'] = cont
                    if r.get('_id'):
                        del res['_id']
                    es.index(body=res,index=r['category'],doc_type="news",id=r['title'])
                except Exception as e:
                    print(e)

# 获取已存在的url
def get_exists_urls():
    my_conn = MongoConn()
    exists_urls_rst = list(my_conn.db['getnews'].find({'source':'清科网'},{'url':1,'_id':0}))
    res = [i['url'] for i in exists_urls_rst]
    return res


# 类别
catinfos = {
    '行业新闻':"http://news.pedata.cn/list_{}_2.html",
    '投资新闻':"http://news.pedata.cn/list_{}_3.html",
    '滚动新闻':"http://news.pedata.cn/news/list_{}_1.html",
    '清科观察':"http://news.pedata.cn/list_{}_0.html",
    '人物专刊':"http://news.pedata.cn/list_{}_9.html"
}

# 映射类别信息
newcats = {
    '人物专刊':'characters'
}

def in_soup(url):
    html = requests.get(url)
    html.encoding = 'utf-8'
    htmltext = html.text
    souprst = BeautifulSoup(htmltext,features='lxml')
    return  souprst

# 获取最新文章url
def get_urls(cat, n):
    base_url = catinfos[cat]
    urls = [base_url.format(i) for i in range(1,n+1)]
    frst = {'cat':None,'val':None}
    result = []
    for url in urls:
        soup = in_soup(url)
        rst0 = soup.find_all(attrs={'class':'new_news_one'})
        for item in rst0:
            imgurl = item.find(attrs={'class':'new_news_one_left'}).a.img['src']
            titleImg = str(uuid.uuid1())
            rst = item.find(attrs={'class':'new_news_one_right'})
            href = rst.h2.a['href']
            inres = (titleImg,imgurl,href)
            if inres not in result:
                result.append(inres)
    frst['cat'] = newcats[cat]
    frst['val'] = reversed(result)
    return frst

# 获取文章标题，发表时间，内容，标签
def get_want(url,cat,imgpath):
    rst = {}
    exsits_urls = get_exists_urls()
    rst['url'] = url[2]
    souprst = in_soup(url[2])
    if souprst and not souprst.find(attrs={'class':'new_news_title'}) and url[0] not in exsits_urls:
        print("正在爬取:",url[2])
        rst['title'] = souprst.title.string
        rst['titleImg'] = url[0]
        rst['description'] = souprst.find(attrs={'class':'article_zy'}).string
        titlepic = requests.get(url[1], stream=True)
        with open(os.path.join(imgpath, url[0]), 'wb') as f:
            for chunk in titlepic.iter_content(128):
                f.write(chunk)
        try:
            rst['author'] = souprst.find(attrs={'class':'article_from'}).find_all('span')[-1].string
        except IndexError:
            rst['author'] = ""
        details00 = souprst.find(attrs={'class':'article_time left'})
        if details00:
            details01 = details00.find_all('span')
            date_1 = re.sub('</?span>','',str(details01[0]))
            time_1 = re.sub('</?span>','',str(details01[1]))
            rst['dateTime'] = "    ".join([date_1,time_1])
        contents = souprst.find(attrs={'class':'article_main'})
        raw_cont = contents.find_all("p")
        rst_cont = []
        for item in raw_cont:
            if item.find("img"):
                src = item.find('img')
                if src['src'].startswith('//'):
                    picurl = "http:" + src['src']
                else:
                    picurl = src['src']
                name = str(uuid.uuid1())
                path = imgpath
                bpic = requests.get(picurl, stream=True)
                with open(os.path.join(path, name ), 'wb') as f:
                    for chunk in bpic.iter_content(128):
                        f.write(chunk)
                piccont = ''.join([str(i).strip() for i in item.contents]).replace(src['src'], '###' + name )
                rst_cont.append(piccont)
            else:
                rst_cont.append(''.join([str(i).strip() for i in item.contents]))
        rst['content'] = [i for i in rst_cont if i != "<br/>" and i]
        rst['createTime'] = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
        rst['timestamp'] = int(datetime.now().timestamp()*1000)
        rst['category'] = cat
        rst['source'] = '清科网'
        raw_tag = souprst.find(attrs={'class':'article_label right'}).find_all('span')
        rst_tag = [re.sub('</?span>','',str(i)) for i in raw_tag]
        rst['tag'] = rst_tag
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
        for it in rst1_add:
            try:
                my_conn.db['getnews'].insert_one(it)
                success_item += 1
            except DuplicateKeyError:
                pass
            except Exception as e:
                print("插入失败",str(e))
    return  success_item

def qingke0(cats,n,imgpath):
    if isinstance(cats,str):
        assert cats in catinfos.keys(),'类别不在范围内'
    allrst = []
    if isinstance(cats,list) and len(cats) > 0:
        urls = [get_urls(cat,n) for cat in cats]
    elif isinstance(cats,str) and cats:
        urls = list(get_urls(cats,n))
    else:
        return -1
    for url in urls:
        cat = url['cat']
        for v in url['val']:
            rst = get_want(v,cat,imgpath)
            allrst.append(rst)
    in_es(allrst)
    res = in_db(allrst)
    return res

def qingke():
    res = qingke0(['人物专刊'],1,"/data0/getnews/images")
    return res


if __name__ == '__main__':
    res = qingke()
    print(res)
    # res = get_urls('人物专刊',2)
    # print(res)
