#!/usr/bin/env python3
import unittest
import unittest_sorter
# import time
import transactor
from transactor import priority_deque


class TestDequePool(unittest.TestCase):

    def test_create(self):
        self.assertTrue(priority_deque())

    def test_pushpop(self):
        dq = priority_deque()
        for p in transactor.priority:
            dq.push(1, nice=p)
            self.assertTrue( 1 == dq.peek_spec(p) )
            self.assertTrue( 1 == dq.pop_spec(p) )


unittest_sorter.main(scope=globals().copy())
