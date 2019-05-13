#! /usr/bin/env python
# coding: utf-8
# Licensed under the MIT License.

import logzero
from logzero import logger
import os
from dotenv import load_dotenv, find_dotenv
from Storage.Mongo import Mongo
from utils.tools import Tool
from utils.tools import get_best_node,big_or_little
from Block.RpcClient import RpcClient
from bson.decimal128 import Decimal128
from pymongo import MongoClient, DESCENDING, ASCENDING
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import time


logzero.logfile(os.getcwd() + "/log/main.log", maxBytes=1e10, backupCount=1)
load_dotenv(find_dotenv(), override=True)
m = Mongo(os.environ.get('MONGODB'), os.environ.get('DB'))


node = get_best_node()
b = RpcClient(node)

def create_index():
        m.connection()['balance'].create_index(
        [('assetId', ASCENDING),('address', ASCENDING)], unique=True)
        m.connection()['balance'].create_index([('balance', DESCENDING)])


def main(start,end):
    while start < end - 5:               
        print("m_balance_status",start)


        m_transaction = m.connection()['transaction'].find({'blockIndex':start}) 
        # print("m_transaction",m_transaction)

        for result in m_transaction: 
            # print("m_transaction:result",result)          
            for vin in result["vin"]:
                handle_utxo(vin["utxo"]["address"],vin["utxo"]["asset"],result["blockIndex"])

            for vout in result["vout"]:
                handle_utxo(vout["address"],vout["asset"],result["blockIndex"])

            if 'nep5' not in result.keys():
                continue

            if result["nep5"] is None:
                continue

            for nep5 in result["nep5"]:
                handle_nep5(nep5["to"],nep5["assetId"],result["blockIndex"])
                handle_nep5(nep5["from"],nep5["assetId"],result["blockIndex"])

        m.connection()['balanceStatus'].update_one({},{"$set":{"index":start+1}})
        start = start + 1


def async_asset_rank():
    try:

        create_index()


        m_balance_status = m.connection()['balanceStatus'].find_one({}) 

        r = b.get_block_count()

        

        if m_balance_status is None:
            m.connection()['balanceStatus'].insert_one({
                "index":0
            })
            return

        main(m_balance_status["index"],r-5)

        # print("r",r)
       
    except Exception as e:
        logger.exception(e)


def handle_utxo(address,asset_id,blockIndex):

    r = b.get_account_state(address)

    # print("handle_utxo",r)
    for balance in r["balances"]:
        if balance["asset"] == asset_id:
            print("balance",balance["asset"],balance["value"])
            save_mongo(asset_id,address,balance["value"],blockIndex)
            break


def handle_nep5(address,asset_id,blockIndex):
    
    r_balance = b.invokefunction_balanceOf(asset_id,big_or_little(Tool.address_to_scripthash(address)))

    if r_balance == 0:
        return

    # print('r_balance',r_balance)

    if "FAULT" in r_balance['state']:
        return

    r_decimals = b.get_nep5_decimals(asset_id)

    if "FAULT" in r_decimals['state']:
        return

    decimals = r_decimals["stack"][0]["value"] or 8

    if(r_balance['stack'][0]['type'] == "ByteArray"):
        value = Tool.hex_to_num_str(
            r_balance['stack'][0]['value'], decimals)
        save_mongo(asset_id,address,value,blockIndex)
        # print('value',value)

    if(r_balance['stack'][0]['type'] == "Integer"):
        value = Tool.hex_to_num_intstr(
            r_balance['stack'][0]['value'], decimals)
        save_mongo(asset_id,address,value,blockIndex)
        # print('value',value)


def save_mongo(asset_id,address,value,blockIndex):
    print("save_mongo",value)
    if value == 0:
        return

    try:
        Decimal128(value)
    except Exception as e:
        logger.exception(e)
        return

    m_balance = m.connection()['balance'].find_one({
                   "assetId" : asset_id,
                    "address" : address,
        })

    if m_balance is None:
        # print("1")
        m.connection()['balance'].insert_one({
            "assetId" : asset_id,
            "address" : address,
            "balance" : Decimal128(str(value)) or 0,
            "blockIndex":blockIndex
        })
        return
        
        
    if  m_balance["blockIndex"] > blockIndex:
        # print("2")
        return
        # print("2")
        


    # print("3")
    m.connection()['balance'].update_one({
            "assetId" : asset_id,
            "address" : address
    },  {
        "$set":{
            "assetId" : asset_id,
            "address" : address,
            "balance" : Decimal128(str(value))  or 0 ,
            "blockIndex":blockIndex
        }
    })
             


if __name__ == "__main__":
    try:
        # async_asset_rank()
      # handle_nep5("AJdhvVSxHJixAyw7xn38XbeHjCFmto5SVX","0x08e8c4400f1af2c20c28e0018f29535eb85d15b6",10)
        sched = BlockingScheduler()
        sched.add_job(async_asset_rank, 'interval', seconds=40)
        sched.start()
    except Exception as e:
        logger.exception(e)
        time.sleep(30)
        sched.start()