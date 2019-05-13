#! /usr/bin/env python
# coding: utf-8
# Licensed under the MIT License.

import requests
from logzero import logger
import os
import random
from utils.tools import get_best_node


class RpcClient(object):
    def __init__(self,node_url):
        self.url = get_best_node()
        print('__init__')

    # 获取主链中区块的数量。
    def get_block_count(self):
        self.url = get_best_node()
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getblockcount",
            "params": [],
            "id": 1
        })
        if r.json()['result'] is None:
            self.get_block_count()       

        # if r.json()['error'] is not None:
        #     self.get_block_count() 

        return r.json()['result']

    def get_block(self,index):
        self.url = get_best_node()
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getblock",
            "params": [index,1],
            "id": 1
        })
        if r.json()['result'] is None:
            self.get_block(index)

        # if r.json()['error']:
        #     self.get_block(index)

        return r.json()['result']  

    # 根据指定的 NEP-5 交易 ID 获取合约日志。
    def get_application_log(self,txid):
        self.url = get_best_node()
        print('get_application_log',self.url)
        try:
            r = requests.post(self.url, json={
                "jsonrpc": "2.0",
                "method": "getapplicationlog",
                "params": [txid],
                "id": 1
            })

            if r.json() is None:
                return None

            if 'result' in r.json():
                return r.json()['result']
             
            return None
        except Exception as e:
            logger.error('error txid %s',txid)
            logger.exception(e)
            return None

        

    # 根据指定的散列值，返回对应的交易信息
    def get_raw_transaction(self,txid):
        self.url = get_best_node()
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getrawtransaction",
            "params": [txid,1],
            "id": 1
        })

        if r.json()['result'] is None:
            self.get_raw_transaction(txid)

        # if r.json()['error']:
        #     return None

        return r.json()['result']

    # 根据指定的资产编号，查询资产信息。
    def get_asset_state(self,asset_id):
        self.url = get_best_node()
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getassetstate",
            "params": [asset_id],
            "id": 1
        })
        if r.json()['result'] is None:
            self.get_asset_state(asset_id)

        # if r.json()['error']:
        #     self.get_asset_state(asset_id)

        return r.json()['result']

    # get decimals
    def get_nep5_decimals(self,asset_id):
        self.url = get_best_node()
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "invokefunction",
            "params": [
                asset_id,
                "decimals", 
                []
                ],
            "id": 2
        })
        if r.json()['result'] is None:
            self.get_nep5_decimals(asset_id)

        return r.json()['result']

    # get 根据账户地址，查询账户全局资产（如 NEO、GAS 等）资产信息。
    def get_account_state(self,address):
        self.url = get_best_node()
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getaccountstate",
            "params": [address],
            "id": 1
        })
        if r.json()['result'] is None:
            self.get_account_state(address)

        return r.json()['result']

    def invokefunction_decimals(self,asset_id):
        self.url = get_best_node()
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "invokefunction",
            "params": [
                asset_id,
                "decimals",
                []
                ],
            "id": 2
        })
        if r.json()['result'] is None:
            self.invokefunction_decimals(asset_id)

        return r.json()['result']   

    def invokefunction_balanceOf(self,asset_id,address_hash160):
        self.url = get_best_node()
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "invokefunction",
            "params": [
                 asset_id,
                "balanceOf",
                [
                {
                    "type": "Hash160",
                    "value": address_hash160
                }
                ]
            ],
            "id": 3
        })
        if 'error' in r.json().keys():
            return 0

        if r.json()['result'] is None:
            self.invokefunction_balanceOf(asset_id,address_hash160)

        return r.json()['result']  

    # get url 
    def get_node(self):
        return self.url

if __name__ == "__main__":
    pass