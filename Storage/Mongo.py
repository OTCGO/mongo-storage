#! /usr/bin/env python
# coding: utf-8
# Licensed under the MIT License.

from pymongo import MongoClient

class Mongo:
    def __init__(self):
        print('__init__')
        self.url = 'mongodb://127.0.0.1:27017/'
        self.db = 'testnet-node'
    def connection(self):
        # print('connet')
        return MongoClient(self.url)[self.db] 

    def find(self,collection):
        print('find')
        self.client.collection.find_one()

    def find_one(self):
        print('find_one') 
    
