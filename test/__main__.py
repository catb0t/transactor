#!/usr/bin/env python3
# Copyright Olivia Stevens 2024

import unittest
import transactor
# import time
from transactor import priority_deque, priority
from .unittest_sorter import test_sorter_main


class TestPriorityDeque(unittest.TestCase):

    def test_create(self):
        self.assertTrue(priority_deque())

    def test_pushpop(self):
        dq = priority_deque()
        for p in priority:
            dq.push(p, want_nice=p)
            self.assertTrue( p == dq.peek( force_nice=(True, p) )[1] )
        self.assertTrue(repr(dq) == "{<priority.undef: -1>: deque([<priority.undef: -1>]), <priority.low: 0>: deque([<priority.low: 0>]), <priority.normal: 1>: deque([<priority.normal: 1>]), <priority.high: 2>: deque([<priority.high: 2>], maxlen=50), <priority.airmail: 3>: deque([<priority.airmail: 3>], maxlen=10)}") # noqa


class TestClerks(unittest.TestCase):

    def test_create(self):
        self.assertTrue(transactor.request_clerk())

    def test_reg_ret(self):
        r = transactor.read_clerk()
        key = "cat"
        r.register_read({
            ~r.fields.uuid: key,
            ~r.fields.nice: transactor.priority.normal,
            ~r.fields.default_get: "users"
        })
        r.do_serve_request()
        print(r.get_response(key, keep=True))
        self.assertTrue(r.get_response(key) == "users")
        self.assertTrue(r.get_descr(key)[~r.fields.uuid] == key)
        self.assertFalse(any(r.have_waiting()))


test_sorter_main(scope=globals().copy())
