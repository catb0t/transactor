#!/usr/bin/env python3
import unittest
import unittest_sorter
import transactor
# import time
from prioritydeque import priority_deque, priority


class TestPriorityDeque(unittest.TestCase):

    def test_create(self):
        self.assertTrue(priority_deque())

    def test_pushpop(self):
        dq = priority_deque()
        for p in priority:
            dq.push(p, nice=p)
            self.assertTrue( p == dq.peek( force_nice=(True, p) )[1] )
        self.assertTrue(repr(dq) == "{<priority.undef: -1>: deque([<priority.undef: -1>]), <priority.low: 0>: deque([<priority.low: 0>]), <priority.normal: 1>: deque([<priority.normal: 1>]), <priority.high: 2>: deque([<priority.high: 2>], maxlen=50), <priority.airmail: 3>: deque([<priority.airmail: 3>], maxlen=10)}") # noqa


class TestClerks(unittest.TestCase):

    def test_create(self):
        self.assertTrue(transactor.request_clerk())

    def test_reg_ret(self):
        r = transactor.read_clerk()
        key = "cat"
        r.register_read({
            "uuid": key,
            "nice": transactor.priority.normal,
            "dbname": "users"
        })
        r.do_serve_request()


unittest_sorter.main(scope=globals().copy())
