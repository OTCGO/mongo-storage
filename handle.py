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

from  utils.redisHelper import RedisHelper



load_dotenv(find_dotenv(), override=True)

print('RPC', os.environ.get('RPC'))
print('MONGODB', os.environ.get('MONGODB'))
print('DB', os.environ.get('DB'))
print('WORK_COUNT', cpu_count())
# b = RpcClient(os.environ.get('RPC'))
m = Mongo(os.environ.get('MONGODB'), os.environ.get('DB'))
obj = RedisHelper(os.environ.get('REDIS'))


chan = "nep5"

# create index
def create_index():
    m.connection()['block'].create_index([('index', DESCENDING)], unique=True)

    m.connection()['transaction'].create_index(
        [('txid', ASCENDING)], unique=True)
    m.connection()['transaction'].create_index([('type', ASCENDING)])
    m.connection()['transaction'].create_index([('blockIndex', ASCENDING)])
    m.connection()['transaction'].create_index(
        [('vin.utxo.address', ASCENDING)])
    m.connection()['transaction'].create_index(
        [('vin.utxo.asset', ASCENDING)])    
    m.connection()['transaction'].create_index([('vout.address', ASCENDING)])
    m.connection()['transaction'].create_index([('vout.asset', ASCENDING)])

    m.connection()['transaction'].create_index([('nep5.to', ASCENDING)])
    m.connection()['transaction'].create_index([('nep5.from', ASCENDING)])
    m.connection()['transaction'].create_index([('nep5.assetId', ASCENDING)])

    m.connection()['address'].create_index(
        [('address', DESCENDING)], unique=True)
    m.connection()['address'].create_index([('blockIndex', ASCENDING)])    

    m.connection()['asset'].create_index(
        [('assetId', DESCENDING)], unique=True)
    m.connection()['asset'].create_index([('blockIndex', ASCENDING)])    



    m.connection()['state'].create_index([('index', DESCENDING)])
    m.connection()['state'].delete_many({})
    m.connection()['state'].insert_one(
        {'_id': ObjectId('5a95047efc2a4961941484e6'), 'height': 0})


def del_all():
    m.connection()['block'].delete_many({})
    m.connection()['transaction'].delete_many({})
    m.connection()['address'].delete_many({})
    m.connection()['asset'].delete_many({})
    m.connection()['state'].delete_many({})


def save_block(b,start, length):
    print('start', start)
    print('length', length)

    try:
        index = start
        while index <= start + length:
            #print('index', index)
            # 判断 m_block 是否已经存在
            m_block = m.connection()['block'].find_one({
                'index': index
            })
            # print('m_block', m_block)
            if m_block is None:
                block = b.get_block(index)
                m_block = block
                #print('block', block)
                m.connection()['block'].insert_one(block)

            # m_block =  m_block or b.get_block(index)
            for tx in m_block['tx']:
                # print('tx',tx['txid'])
                # 判断 m_transaction 是否已经存在
                m_transaction = m.connection()['transaction'].find_one({
                    'txid': tx['txid']
                })
                if m_transaction is None:
                    save_transaction(b, tx, m_block['index'])

                # 保存address
                save_address(b, tx, m_block['index'])

            index = index + 1
        return True
    except Exception as e:
        logger.error('save_block index %s', index)
        logger.exception(e)
        time.sleep(1)
        save_block(b,start, length)
        # m.connection()['state'].insert_one({
        #     'index': index,
        #     'error': True
        # })


def save_transaction(b, tx, blockIndex):

    # InvocationTransaction 需要单独处理
    if tx['type'] == 'InvocationTransaction':
        tx['nep5'] = handle_nep5(b,tx['txid'], blockIndex) or []
        print('nep5', tx['nep5'])

    for vin in tx['vin']:
        utxo = b.get_raw_transaction(vin['txid'])['vout'][vin['vout']]
        vin['utxo'] = utxo

    tx['blockIndex'] = blockIndex
    #print('tx', tx)
    m.connection()['transaction'].insert_one(tx)


def save_address(b, tx, blockIndex):
    for vout in tx['vout']:
        #print('save_address', vout)
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
        m_assert = m.connection()['asset'].find_one({
            'assetId': vout['asset']
        })
        if m_assert is None:
            save_assert(b, vout['asset'],blockIndex)


def save_assert(b, assetId,blockIndex):
    r = b.get_asset_state(assetId)
    r['assetId'] = r['id']
    r['blockIndex'] = blockIndex
    del r['id']
    m.connection()['asset'].insert_one(r)


def save_state():
    pass


def handle_nep5(b,txid, blockIndex):
    print("handle_nep5",txid)
    # 0x9db4725a8b6a43ce91d5085fe88df59578993d7cd0b2397934215463c48d575f
    try:
        r = b.get_application_log(txid)
        # print("r",r)
        nep5_arr = []
        if r is not None:
            if 'notifications' in r:
                # print("r",r)
                # vmstate"是虚拟机执行合约后的状态，如果包含"FAULT"的话，
                if "FAULT" in r['vmstate']:
                    return 
                for item in r['notifications']:
                    # not transfer
                    if item['state']['value'][0]['value'] != "7472616e73666572":
                        break
                    if 'contract' in item and 'state' in item and 'value' in item['state'] and len(item['state']['value']) == 4:
                        # handle decimals
                        decimals = b.get_nep5_decimals(item['contract'])['stack'][0]['value'] or 0
                        print('decimals',decimals)

                        # print('handle_nep5')
                        nep5_assert = m.connection()['asset'].find_one({
                            "assetId": item['contract'],
                        })
                        #print('nep5_assert', nep5_assert)
                        # asserts
                        if nep5_assert is None:
                            m.connection()['asset'].insert_one({
                                "assetId": item['contract'],
                                'blockIndex': blockIndex,
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

                            if(item['state']['value'][3]['type'] == "ByteArray"):
                                value = Tool.hex_to_num_str(item['state']['value'][3]['value'],decimals)

                            if(item['state']['value'][3]['type'] == "Integer"):
                                value =  Tool.hex_to_num_intstr(item['state']['value'][3]['value'],decimals)

                            obj.public(chan,address_to)

                            nep5_arr.append({
                                # "txid": txid,
                                "assetId": item['contract'],
                                "operation": 'mintTokens',
                                # 转出 为空
                                "from": '',
                                # 输入
                                "to": address_to,
                                "value": value,
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

                            # handle value
                            if(item['state']['value'][3]['type'] == "ByteArray"):
                                value = Tool.hex_to_num_str(item['state']['value'][3]['value'],decimals)

                            if(item['state']['value'][3]['type'] == "Integer"):
                                value =  Tool.hex_to_num_intstr(item['state']['value'][3]['value'],decimals)


                            obj.public(chan,address_to) 
                            obj.public(chan,address_from) 

                            # print('value',value)
                            nep5_arr.append({
                                # "txid": txid,
                                "assetId": item['contract'],
                                "operation": 'transfer',
                                # 转出
                                "from": address_from,
                                # 输入
                                "to": address_to,
                                "value": value,
                            })

                            


        # print('nep5_arr',nep5_arr)
        return nep5_arr

    except Exception as e:
        print("Exception",e)
        logger.error('handle_nep5 txid %s', txid)
        return []
