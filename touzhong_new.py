# -*- coding:UTF-8 -*-
import requests
import io
import os
import sys 
import json
import re
import time
import datetime
from elasticsearch import Elasticsearch
from datetime import datetime
import uuid
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from connmongo import MongoConn
from pymongo.errors import DuplicateKeyError


base_url = "http://www.chinaventure.com.cn/"

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
                es.index(body=res,index=res['category'],doc_type="news",id=res['title'])
            except Exception as e:
                print("添加到es失败",e)
         

# 类别信息
catinfos = {
    '11':'VC/PE','3':'瞰三板','20':'产业资本',
    '14':'锐公司','5':'金融','4':'潮汛',
    '23':'人物','2':'原创','00':'投融动态'
}

# 映射类别信息
newcats = {
    '11':'vc_pe',
    '20':'industry_news',
    '14':'big_companies',
    '23':'characters'
}

# ['industry_news','vc_pe','angel','entrepreneur','big_companies','characters','finance_realestate']

# 根据类别/想获取的条目数来生成访问url
def geturl(catid,itemrange):
    assert catid in catinfos.keys(),'catid不正确'
    base_url = "http://www.chinaventure.com.cn/cmsmodel/news/"
    frst = {}
    if catid == '00':
        cat = 'jsonListByEvent'
        rst_url = base_url + cat + "/" + itemrange + ".do"
    else:
        cat = 'jsonListByChannel'
        rst_url = base_url + cat + "/" + str(catid) + "/" + itemrange + ".shtml"
    frst['cat'] = newcats[catid]
    frst['val'] = rst_url
    return frst

# 根据实际url来提取信息(投融动态)
def getcont(url,cat,picpath):
    rst_all = []
    if url:
        print("正在爬取:",url)
        rst = requests.get(url)
        rst2 = eval(rst.content)
        if rst2['status'] == 100000:
            for item in rst2['data']:
                rst_cont = {}
                rst_cont['dateTime'] = datetime.strftime(datetime.fromtimestamp(item['news']['publishAt']/1000),"%Y-%m-%d %H:%M:%S")
                rst_cont['title'] = item['news']['title']
                titleImg = str(uuid.uuid1())
                rst_cont['titleImg'] = titleImg
                imgurl = "https://pic.chinaventure.com.cn/" + item['news']['coverImg']
                titlepic = requests.get(imgurl,stream=True)
                with open(os.path.join(picpath,titleImg),'wb') as f:
                    for chunk in titlepic:
                        f.write(chunk)
                rst_cont['description'] = item['news']['introduction']
                rst_cont['author'] = item['news']['author']
                rst_cont['tag'] = ','.join([item['keyword'] for item in item['keywordList']])
                rst_cont['url'] = "https://www.chinaventure.com.cn/cmsmodel/news/detail/{}.shtml".format(item['news']['id'])
                content = []
                raw_cont = re.split("</?p>",item['news']['content'])
                for i in raw_cont:
                    url = re.findall('src=\"(.*)\"',i)
                    if url:
                        url = str(url[0])
                        picname = str(uuid.uuid1())
                        bpic = requests.get(url,stream=True)
                        with open(os.path.join(picpath,picname ), 'wb') as f:
                            for chunk in bpic:
                                f.write(chunk)
                        content.append(re.sub('src=\".*\"',"src=###path" + picname ,i))
                    else:
                        content.append(i)
                rst_cont['content'] = [i for i in content if i.strip()]
                rst_cont['url'] = url
                rst_cont['source'] = '投中网'
                rst_cont['category'] = cat
                rst_cont['createTime'] = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
                rst_cont['timestamp'] = item['news']['publishAt']
                rst_all.append(rst_cont)
    return rst_all

def getcont2(url,cat,picpath):
    rst_all = []
    if url:
        print("正在爬取:",url)
        rst = requests.get(url)
        rst2 = eval(rst.content)
        if rst2['status'] == 100000:
            for item in rst2['data']['list']:
                rst_cont = {}
                rst_cont['dateTime'] = datetime.strftime(datetime.fromtimestamp(item['news']['publishAt']/1000),"%Y-%m-%d %H:%M:%S")
                rst_cont['title'] = item['news']['title']
                titleImg = str(uuid.uuid1())
                rst_cont['titleImg'] = titleImg
                imgurl = "https://pic.chinaventure.com.cn/" + item['news']['coverImg']
                titlepic = requests.get(imgurl,stream=True)
                with open(os.path.join(picpath,titleImg),'wb') as f:
                    for chunk in titlepic:
                        f.write(chunk)
                rst_cont['description'] = item['news']['introduction']
                rst_cont['author'] = item['news']['author']
                rst_cont['tag'] = ','.join([item['keyword'] for item in item['keywordList']])
                rst_cont['url'] = "https://www.chinaventure.com.cn/cmsmodel/news/detail/{}.shtml".format(item['news']['id'])
                content = []
                raw_cont = re.split("</?p>",item['news']['content'])
                for i in raw_cont:
                    url = re.findall('src=\"(.*)[/s\?]"',i)
                    if url:
                        url = str(url[0])
                        picname = str(uuid.uuid1())
                        bpic = requests.get(url,stream=True)
                        with open(os.path.join(picpath,picname ), 'wb') as f:
                            for chunk in bpic:
                                f.write(chunk)
                        content.append(re.sub('src=\".*\"',"src=###path" + picname ,i))
                    else:
                        content.append(i)
                rst_cont['content'] = [i for i in content if i.strip()]
                rst_cont['source'] = '投中网'
                rst_cont['category'] = cat
                rst_cont['createTime'] = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
                rst_cont['timestamp'] = int(datetime.now().timestamp()*1000)
                rst_all.append(rst_cont)
    return rst_all

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
        rst1_add = [item for item in allrst ]
        print("正在存入数据库，新增%s条" % len(rst1_add))
        for it in rst1_add:
            try:
                my_conn.db['getnews'].insert_one(it)
                success_item += 1
            except DuplicateKeyError:
                pass
            except Exception as e:
                print("参数为空值，插入失败",str(e))

    return  success_item

def final(catids,itemrange,picpath):
    allrst = []
    for id in catids:
        urlinfo = geturl(id,itemrange)
        if urlinfo and id != '00':
            rst = getcont2(urlinfo['val'],urlinfo['cat'],picpath)
            allrst.extend(rst)
        if urlinfo and id == '00':
            rst = getcont(urlinfo['val'],urlinfo['cat'], picpath)
            allrst.extend(rst)
    in_es(allrst)
    res =in_db(allrst)
    return res

def touzhong():
    res = final(['11','20','14','23'],'0-12',"/data0/getnews/images")
    return res

if __name__ == '__main__':
    res = touzhong()
    print(res)
