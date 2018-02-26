#! /usr/bin/env python
# coding: utf-8
# Licensed under the MIT License.

import requests


class RpcClient(object):
    def __init__(self,node_url):
        self.url = node_url
        print('__init__')

    # 获取主链中区块的数量。
    def get_block_count(self):
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getblockcount",
            "params": [],
            "id": 1
        })
        return r.json()['result']
    def get_block(self,index):
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getblock",
            "params": [index,1],
            "id": 1
        })
        return r.json()['result']

    # 根据指定的 NEP-5 交易 ID 获取合约日志。
    def get_application_log(self,txid):
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getapplicationlog",
            "params": [txid],
            "id": 1
        })
        return r.json()['result']

    # 根据指定的散列值，返回对应的交易信息
    def get_raw_transaction(self,txid):
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getrawtransaction",
            "params": [txid,1],
            "id": 1
        })
        return r.json()['result']

    # 根据指定的资产编号，查询资产信息。
    def get_asset_state(self,asset_id):
        r = requests.post(self.url, json={
            "jsonrpc": "2.0",
            "method": "getassetstate",
            "params": [asset_id],
            "id": 1
        })
        return r.json()['result']


if __name__ == "__main__":
    pass