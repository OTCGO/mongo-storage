#! /usr/bin/env python
# coding: utf-8
# Licensed under the MIT License.

from pymongo import MongoClient, DESCENDING, ASCENDING
from Block.RpcClient import RpcClient
from Storage.Mongo import Mongo
from bson.objectid import ObjectId
import time
import os
from multiprocessing import Pool, cpu_count
from binascii import unhexlify
from utils.tools import Tool
from binascii import unhexlify
# import argparse
from dotenv import load_dotenv, find_dotenv
import logzero
from logzero import logger


load_dotenv(find_dotenv(), override=True)
logzero.logfile(os.getcwd() + "/log/async.log", maxBytes=1e6, backupCount=3)




# parser = argparse.ArgumentParser()
# parser.add_argument("-m", "--mongodb",help="verify database name, default antshares", default='mongodb://127.0.0.1:27017/')
# parser.add_argument("-d", "--db",help="verify collections name, default antshares", default='testnet-node')
# parser.add_argument("-r", "--rpc",help="verify collections name, default antshares", default='http://future.otcgo.cn:20332')

# args = parser.parse_args()

print('RPC', os.environ.get('RPC'))
print('MONGODB', os.environ.get('MONGODB'))
print('DB', os.environ.get('DB'))
b = RpcClient(os.environ.get('RPC'))
m = Mongo(os.environ.get('MONGODB'),os.environ.get('DB'))

# create index
def create_index():
    m.connection()['block'].create_index([('index', DESCENDING)], unique=True)

    m.connection()['transaction'].create_index(
        [('txid', ASCENDING)], unique=True)
    m.connection()['transaction'].create_index([('type', ASCENDING)])
    m.connection()['transaction'].create_index([('blockIndex', ASCENDING)])
    m.connection()['transaction'].create_index(
        [('vin.utxo.address', ASCENDING)])
    m.connection()['transaction'].create_index([('vou.address', ASCENDING)])

    m.connection()['address'].create_index([('address', DESCENDING)], unique=True)

    m.connection()['assert'].create_index(
        [('assetId', DESCENDING)], unique=True)

    m.connection()['state'].create_index([('index', DESCENDING)]) 
    m.connection()['state'].delete_many({})
    m.connection()['state'].insert_one({'_id':ObjectId('5a95047efc2a4961941484e6'),'height': 0})    


def del_all():
    m.connection()['block'].delete_many({})
    m.connection()['transaction'].delete_many({})
    m.connection()['address'].delete_many({})
    m.connection()['assert'].delete_many({})
    m.connection()['state'].delete_many({})


def save_block(start, length):
    print('start', start)
    print('length', length)

    try:
        index = start
        while index <= start + length:
            print('index', index)
            # 判断 m_block 是否已经存在
            m_block = m.connection()['block'].find_one({
                'index': index
            })
            print('m_block', m_block)
            if m_block is None:
                block = b.get_block(index)
                m_block = block
                print('block', block)
                m.connection()['block'].insert_one(block)

            # m_block =  m_block or b.get_block(index)
            for tx in m_block['tx']:
                # 判断 m_transaction 是否已经存在
                m_transaction = m.connection()['transaction'].find_one({
                    'txid': tx['txid']
                })
                if m_transaction is None:
                    save_transaction(tx, m_block['index'])

                # 保存address
                save_address(tx, m_block['index'])

            index = index + 1

            return True
    except Exception as e:
        logger.exception(e)
        time.sleep(1)
        save_block(start, length)
        # m.connection()['state'].insert_one({
        #     'index': index,
        #     'error': True    
        # })
        




def save_transaction(tx, blockIndex):

    # InvocationTransaction 需要单独处理
    if tx['type'] == 'InvocationTransaction':
        tx['nep5'] = handle_nep5(tx['txid'], blockIndex) or []
        print('nep5', tx['nep5'])

    for vin in tx['vin']:
        utxo = b.get_raw_transaction(vin['txid'])['vout'][vin['vout']]
        vin['utxo'] = utxo

    tx['blockIndex'] = blockIndex
    print('tx', tx)
    m.connection()['transaction'].insert_one(tx)


def save_address(tx, blockIndex):
    for vout in tx['vout']:
        print('save_address', vout)
        # 判断 m_address 是否已经存在
        m_address = m.connection()['address'].find_one({
            'address': vout['address']
        })
        if m_address is None:
            m.connection()['address'].insert_one({
                'address': vout['address'],
                'blockIndex': blockIndex
            })

        # 判断 m_assert 是否已经存在
        m_assert = m.connection()['assert'].find_one({
            'assetId': vout['asset']
        })
        if m_assert is None:
            save_assert(vout['asset'])


def save_assert(assetId):
    r = b.get_asset_state(assetId)
    r['assetId'] = r['id']
    del r['id']
    m.connection()['assert'].insert_one(r)


def save_state():
    pass


def handle_nep5(txid, blockIndex):
    # 0x9db4725a8b6a43ce91d5085fe88df59578993d7cd0b2397934215463c48d575f
    r = b.get_application_log(txid)
    print('get_application_log', r)
    nep5_arr = []
    if r['notifications']:
        for item in r['notifications']:
            nep5_assert = m.connection()['assert'].find_one({
                "assetId": item['contract'],
            })
            print('nep5_assert', nep5_assert)
            # asserts
            if nep5_assert is None:
                m.connection()['assert'].insert_one({
                    "assetId": item['contract'],
                    'type': 'nep5'
                })

            # mintTokens
            if item['state']['value'][1]['value'] == "":
                # 判断地址
                address_to = Tool.scripthash_to_address(
                    unhexlify(item['state']['value'][2]['value']))
                nep5_address_to = m.connection()['address'].find_one({
                    'address': address_to
                })

                if nep5_address_to is None:
                    m.connection()['address'].insert_one({
                        'address': address_to,
                        'blockIndex': blockIndex
                    })

                nep5_arr.append({
                    # "txid": txid,
                    "assetId": item['contract'],
                    "operation": 'mintTokens',
                    # 转出 为空
                    "from": '',
                    # 输入
                    "to": address_to,
                    "value": Tool.hex_to_num_str(item['state']['value'][3]['value']),
                })
            else:
                # 判断地址from
                address_from = Tool.scripthash_to_address(
                    unhexlify(item['state']['value'][1]['value']))
                nep5_address_from = m.connection()['address'].find_one({
                    'address': address_from
                })
                if nep5_address_from is None:
                    m.connection()['address'].insert_one({
                        'address': address_from,
                        'blockIndex': blockIndex
                    })

                 # 判断地址to
                address_to = Tool.scripthash_to_address(
                    unhexlify(item['state']['value'][2]['value']))
                nep5_address_to = m.connection()['address'].find_one({
                    'address': address_to
                })
                if nep5_address_to is None:
                    m.connection()['address'].insert_one({
                        'address': address_to,
                        'blockIndex': blockIndex
                    })
                nep5_arr.append({
                    # "txid": txid,
                    "assetId": item['contract'],
                    "operation": 'transfer',
                    # 转出
                    "from": address_from,
                    # 输入
                    "to": address_to,
                    "value": Tool.hex_to_num_str(item['state']['value'][3]['value']),
                })

    return nep5_arr


def main():

    try:
        start = time.time()

        r = b.get_block_count()
        skip = 1000

        pool = Pool(processes=cpu_count())
        print('count', r - 1)
        # m_state = m.connection()['state'].find_one({},{'index':1},sort = [('index',DESCENDING)]) or { 'index' : -1}


        # print('m_state',m_state)
        for x in range( 0 , r - 1, skip):
            print('x', x)
            # pool.apply_async(save_block, args=(x, skip - 1)) # 非阻塞
            pool.apply_async(save_block, args=(x, skip - 1)) # 阻塞





          

        pool.close()
        pool.join()

        end = time.time()
        print('Took %.3f seconds.' % (end - start))
    except Exception as e:
        print('err', e)
        time.sleep(30)
        main()


def check():
    try:
        while True:
            # block
            r = b.get_block_count()

            m_block = m.connection()['block'].find_one({},{'index':1},sort = [('index',DESCENDING)]) or { 'index' : -1}
            print('m_block',m_block)



            if r - 1 - m_block['index'] < 1001:

                if r - 1 == m_block['index']:
                    return

                print('start check')
                save_block(m_block['index'] + 1 , r - 1 - m_block['index'])


            time.sleep(30)
    except Exception as e:
        logger.exception(e)
        print('err', e)
        time.sleep(30)
        check()


def verify_blocks(start):
    try:
        print('verify_blocks start',start)

        # end = b.get_block_count()

       
        # verify_count = 1000
        # end = start + verify_count

        end = b.get_block_count()


        # if end > b.get_block_count():
        #     return

        print('start',start)
        print('end',end)

        for i in range(start,end):
            print('index',i)
            m_block = m.connection()['block'].find_one({'index': i},{'index':1})
            while m_block is None:
                print('save_block',i)
                result = save_block(i, 0)
                if result:
                    break
                m_block = m.connection()['block'].find_one({'index': i},{'index':1})  
                if m_block:
                    break

            m.connection()['state'].update_one({'_id':ObjectId('5a95047efc2a4961941484e6')},{
                    '$set':{
                        'height': i
                    }
            })    

        # m_block_count = m.connection()['block'].find({'index': { '$gte':start,'$lt': end }},{'index':1}).count()

        # if m_block_count != verify_count:
        #     # pool = Pool(processes=cpu_count())

        #     for i in range(start,end):
        #         print('i',i)
        #         m_block = m.connection()['block'].find_one({'index': i},{'index':1})
        #         if m_block is None:
        #             print('save_block',i)
        #             save_block(i, 0)
        #             m.connection()['state'].update_one({'_id':ObjectId('5a95047efc2a4961941484e6')},{
        #                 '$set':{
        #                     'height': i
        #                 }
        #             })
        #             # pool.apply_async(save_block, args=(i, 0))

            
            
            # pool.close()
            # pool.join()       

            # time.sleep(5)
            # verify_blocks(start)
        # else:


            



            # verify_blocks(end)


        

    except Exception as e:
        logger.exception(e)
        time.sleep(5)
        m_state = m.connection()['state'].find_one({'_id':ObjectId('5a95047efc2a4961941484e6')})
        verify_blocks(m_state['height'])


if __name__ == "__main__":
    # del_all()
    # create_index()
    # main()
    # check()
    verify_blocks(m.connection()['state'].find_one({'_id':ObjectId('5a95047efc2a4961941484e6')})['height'])