#! /usr/bin/env python
# coding: utf-8
# Licensed under the MIT License.


from Block.RpcClient import RpcClient
import time
import os
from multiprocessing import Pool, cpu_count
from binascii import unhexlify
from utils.tools import Tool
from binascii import unhexlify
from dotenv import load_dotenv, find_dotenv
import logzero
from logzero import logger
from handle import save_block,del_all,create_index
from utils.tools import get_random_node

logzero.logfile(os.getcwd() + "/log/main.log", maxBytes=1e10, backupCount=1)
load_dotenv(find_dotenv(), override=True)



work_count = cpu_count()



def main(b):

    try:
        start = time.time()

        # ## random node
        # node = get_best_node()
        # if node == '':
        #     return


        
        r = b.get_block_count()
        print('get_block_count',r)
        skip = 1000

        pool = Pool(processes=work_count)

        for x in range( 0 , r - 1, skip):
            print('x', x)
            pool.apply_async(save_block, args=(b, x, skip - 1)) # 非阻塞



        pool.close()
        pool.join()




          

        end = time.time()
        logger.info('main %.3f seconds.' % (end - start))
    except Exception as e:
        #print('err', e)
        logger.exception(e)
        time.sleep(5)

        b = RpcClient()
        b.url = get_random_node()

        main(b)




if __name__ == "__main__":
    try:
        logger.info('main start')
        b = RpcClient()
        main(b)
    except Exception as e:
        logger.exception(e)
