#! /usr/bin/env python
# coding: utf-8
# Licensed under the MIT License.

import os,sys 
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0,parentdir) 
import unittest
from Block.RpcClient import RpcClient
 

class TestBlock(unittest.TestCase):

    def test_get_block_count(self):
        b = RpcClient()
        self.assertIsNotNone(b.get_block_count())

    def test_get_block(self):
        b = RpcClient()
        index = b.get_block(1186737)['index']
        print(index)
        self.assertAlmostEqual(index,1186737)

    def test_get_asset_state(self):
        b = RpcClient()
        assert_id = '0xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b'
        r = b.get_asset_state(assert_id)
        print(r)
        self.assertAlmostEqual(r['id'],assert_id)

if __name__ == '__main__':
    unittest.main()