#! /usr/bin/env python
# coding: utf-8
# Licensed under the MIT License.

from pymongo import MongoClient, DESCENDING, ASCENDING
from Block.RpcClient import RpcClient
from Storage.Mongo import Mongo
import time
from multiprocessing import Pool, cpu_count
from binascii import unhexlify
from utils.tools import Tool
from binascii import unhexlify
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-m", "--mongodb",help="verify database name, default antshares", default='mongodb://127.0.0.1:27017/')
parser.add_argument("-d", "--db",help="verify collections name, default antshares", default='testnet-node')
parser.add_argument("-r", "--rpc",help="verify collections name, default antshares", default='http://future.otcgo.cn:20332')

args = parser.parse_args()

print('args',args)
b = RpcClient(args.rpc)
m = Mongo(args.mongodb,args.db)

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
                print('block', block)
                m.connection()['block'].insert_one(block)

            m_block =  m_block or b.get_block(index)
            for tx in block['tx']:
                # 判断 m_transaction 是否已经存在
                m_transaction = m.connection()['transaction'].find_one({
                    'txid': tx['txid']
                })
                if m_transaction is None:
                    save_transaction(tx, block['index'])

                # 保存address
                save_address(tx, block['index'])

            index = index + 1
    except Exception as e:
        m.connection()['state'].insert_one({
            'index': index
        })
        pass




def save_transaction(tx, blockIndex):

    # InvocationTransaction 需要单独处理
    if tx['type'] == 'InvocationTransaction':
        tx['nep5'] = handle_nep5(tx['txid'], blockIndex)
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
                "contract": item['contract'],
            })
            print('nep5_assert', nep5_assert)
            # asserts
            if nep5_assert is None:
                m.connection()['assert'].insert_one({
                    "contract": item['contract'],
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
                    "contract": item['contract'],
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
                    "contract": item['contract'],
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

        create_index()
        r = b.get_block_count()
        skip = 1000

        pool = Pool(processes=cpu_count())
        print('count', r - 1)
        m_state = m.connection()['state'].find_one({},{'index':1},sort = [('index',DESCENDING)]) or { 'index' : -1}


        print('m_state',m_state)
        for x in range((m_state['index'] + 1 or 0) , r - 1, skip):
            print('x', x)
            pool.apply_async(save_block, args=(x, skip - 1))





          

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
            r = b.get_block_count()   1

            m_block = m.connection()['block'].find_one({},{'index':1},sort = [('index',DESCENDING)]) or { 'index' : -1}
            print('m_block',m_block)



            if r - 1 - m_block['index'] < 1001:

                if r - 1 == m_block['index']:
                    return
                    
                print('start check')
                save_block(m_block['index'] + 1 , r - 1 - m_block['index'])

            time.sleep(30)
    except Exception as e:
        print('err', e)
        time.sleep(30)
        check()


def verify_blocks():
    try:
        m_state = m.connection()['state'].find_one({},{'index':1},sort = [('index',DESCENDING)]) or { 'index' : 0}
        start = m_state['index']

        end = b.get_block_count()


        print('start',start)
        print('end',end)
        
        pool = Pool(processes=cpu_count())

        for i in range(start,end):
            m_block = m.connection()['block'].find_one({'index': i},{'index':1})
            if m_block is None:
                print('save_block',i)
                pool.apply_async(save_block, args=(i, 0))    
        
        pool.close()
        pool.join()

    except Exception as e:
        print('err', e)
        time.sleep(30)
        verify_blocks()


if __name__ == "__main__":
    # del_all()
    main()
    check()
    verify_blocks()




