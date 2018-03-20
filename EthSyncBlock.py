# -*- coding: utf-8 -*-
from MongoConnSingle import MongoConn
from web3 import Web3, HTTPProvider, IPCProvider
import threading
import traceback
import time

def check_connected(conn):
    #检查是否连接成功
    if not conn.connected:
        # raise NameError, 'stat:connected Error' 
        raise 'stat:connected Error'

def save(table, value):
    # 一次操作一条记录，根据‘_id’是否存在，决定插入或更新记录
    try:
        my_conn = MongoConn()
        check_connected(my_conn)
        my_conn.db[table].save(value)
    except Exception:
        traceback.print_exc()

def insert(table, value):
    # 可以使用insert直接一次性向mongoDB插入整个列表，也可以插入单条记录，但是'_id'重复会报错
    try:
        my_conn = MongoConn()
        check_connected(my_conn)
        my_conn.db[table].insert(value, continue_on_error=True)
    except (Exception) as e:
        # traceback.print_exc()
        print('!!!!!!!insert fail!!!!!!')
        print(e)
        print(value)

def update(table, conditions, value, s_upsert=False, s_multi=False):
    try:
        my_conn = MongoConn()
        check_connected(my_conn)
        my_conn.db[table].update(conditions, value, upsert=s_upsert, multi=s_multi)
    except Exception:
        traceback.print_exc()

def upsert_mary(table, datas):
    #批量更新插入，根据‘_id’更新或插入多条记录。
    #把'_id'值不存在的记录，插入数据库。'_id'值存在，则更新记录。
    #如果更新的字段在mongo中不存在，则直接新增一个字段
    try:
        my_conn = MongoConn()
        check_connected(my_conn)
        bulk = my_conn.db[table].initialize_ordered_bulk_op()
        for data in datas:
            _id=data['_id']
            bulk.find({'_id': _id}).upsert().update({'$set': data})
        bulk.execute()
    except Exception:
        traceback.print_exc()

def upsert_one(table, data):
    #更新插入，根据‘_id’更新一条记录，如果‘_id’的值不存在，则插入一条记录
    try:
        my_conn = MongoConn()
        check_connected(my_conn)
        query = {'_id': data.get('_id','')}
        if not my_conn.db[table].find_one(query):
            my_conn.db[table].insert(data)
        else:
            data.pop('_id') #删除'_id'键
            my_conn.db[table].update(query, {'$set': data})
    except Exception:
        traceback.print_exc()

def find_one(table, value):
    #根据条件进行查询，返回一条记录
    try:
        my_conn = MongoConn()
        check_connected(my_conn)
        return my_conn.db[table].find_one(value)
    except Exception:
        traceback.print_exc()

def find(table, value):
    #根据条件进行查询，返回所有记录
    try:
        my_conn = MongoConn()
        check_connected(my_conn)
        return my_conn.db[table].find(value)
    except Exception:
        traceback.print_exc()

def select_colum(table, value, colum):
    #查询指定列的所有值
    try:
        my_conn = MongoConn()
        check_connected(my_conn)
        return my_conn.db[table].find(value, {colum:1})
    except Exception:
        traceback.print_exc()

def do_sync():
    global timer

    #连接geth
    web3 = Web3(HTTPProvider('http://101.132.189.59:8545'))

    #获得当前链上最高的区块号
    highest_block_num = web3.eth.blockNumber

    #查询已经同步的最高区块
    curr_sync_block_num = 0
    my_conn = MongoConn()
    res = my_conn.db['eth_block_table'].aggregate([{"$group":{"_id": "max", "max_value":{"$max":"$number"}}}]);
    for doc in res:
        curr_sync_block_num = doc['max_value']
        break
    res.close()

    #检查是否需要同步
    if curr_sync_block_num >= highest_block_num:
        return 0
        # sys.exit(1)
    else:
        print ("highest_block_num: ", highest_block_num,
                ", curr_sync_block_num: ", curr_sync_block_num,
                "need to sync block numers: ", highest_block_num - curr_sync_block_num)

    #还需要同步
    for i in range(curr_sync_block_num + 1, highest_block_num):
    # for i in range(125616, 125617):
        block = web3.eth.getBlock(i)

        # print ("block.no: ", i, "transactions:", len(block.transactions))

        #插入trasaction表
        for trans_hash in block.transactions:
            trans = web3.eth.getTransaction(trans_hash)

            # print ("insert trans: ", trans_hash, " in block number: ", i)

            try:
                trans_dict = dict(trans)
                value = trans_dict['value']
                trans_dict['value'] = str(value)

                gasPrice = trans_dict['gasPrice']
                trans_dict['gasPrice'] = str(gasPrice)

                insert('eth_transaction_table', trans_dict)
            except Exception:
                # traceback.print_exc()
                print ("err insert trans: ", trans_hash, " in block number: ", i)
                print (trans_dict)

        #插入block表,'number' 的值必须不重复，否则报错
        try:
            insert('eth_block_table', dict(block))
        except Exception:
            print ("err insert block: ", i, "block.number: ", block.number)
            print (block)

    #重启timer
    timer = threading.Timer(10, do_sync)
    timer.start()

def eth_sync_block():
    timer = threading.Timer(10, do_sync)
    timer.start()

    while True:  
        time.sleep(1)
