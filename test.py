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
from handle import save_block
from utils.tools import get_best_node
import random

logzero.logfile(os.getcwd() + "/log/main.log", maxBytes=1e10, backupCount=1)
load_dotenv(find_dotenv(), override=True)

b = RpcClient(os.environ.get('RPC'))
m = Mongo(os.environ.get('MONGODB'), os.environ.get('DB'))
work_count = cpu_count()

def verify_blocks(start):
    try:

        ## random node
        node = get_best_node()
        if node == '':
            return


        b = RpcClient(node)
        save_block(b, start, 0)
    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    try:
        # print('start',os.environ.get('RPC_ARRAY').split(','))
        print('start',random.choice(os.environ.get('RPC_ARRAY').split(',')))
        # verify_blocks(3392009)
    except Exception as e:
        logger.exception(e)
