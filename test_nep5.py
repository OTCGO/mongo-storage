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
from decimal import Decimal as D
from handle import handle_nep5,save_transaction
from utils.tools import sci_to_str
import math


logzero.logfile(os.getcwd() + "/log/main.log", maxBytes=1e10, backupCount=1)
load_dotenv(find_dotenv(), override=True)

b = RpcClient(os.environ.get('RPC'))
m = Mongo(os.environ.get('MONGODB'), os.environ.get('DB'))
work_count = cpu_count()




if __name__ == "__main__":
    try:
        print('start')
        # handle_nep5('0xbe1bb54ba978a0330c3eafe8f30cb4514db8109e7bdea0a635e9bc664e0e034f',2546788)
        # handle_nep5('0x6f4e4391a0e36c166d04730771b92b3c8464043f2afb1feff859f4f8d6bf3f21',2539283)
        # handle_nep5('0x117fdfffc229386a5f4cd4be7a125e314cebf07b61f01ea5962304b9d2e1f3e1',2546788)


        # d = D(int('277627397'))
        # print(sci_to_str(str(d/(D(math.pow(10,int(8))))))) 
    except Exception as e:
        logger.exception(e)
