# -*- coding: utf-8 -*-
import pymongo
import sys
import traceback

mongo_config = {
    'host':'10.9.60.12',
    'port':27017,
    'db_name':'finance_news',
    'username':None,
    'password':None
}

class MongoConn(object):
    def __init__(self):
        # connect db
        try:
            self.conn = pymongo.MongoClient(mongo_config['host'],mongo_config['port'])
            self.db = self.conn[mongo_config['db_name']]
            self.username = mongo_config['username']
            self.password = mongo_config['password']
            if self.username and self.password:
                self.connected = self.db.authenticate(self.username, self.password)
            else:
                self.connected = True
        except Exception:
            print(traceback.format_exc())
            print("Connect Statics Database Fail.")
            sys.exit(1)


if __name__ == '__main__':
    my_conn = MongoConn()
    datas = [
        {'_id':10,'data':12},
    {'_id':11,'data':22},
    {'_id':12,'data':'cc'}
    ]
    # my_conn.db['news'].insert_many(datas)
    res = my_conn.db['news'].find({},{'url':1})
    rst = []
    for k in res:
        rst.append(k['url'])
    print(rst)