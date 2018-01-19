#!/usr/bin/env python3

import transactor
import threading
import time

R = transactor.read_clerk()


def client(key):
    for i in range(10):
        time.sleep(0)
        R.register_read({
          "uuid": key,
          "nice": transactor.priority.normal,
          "dbname": "users"
        })


def server(key):
    i = 0
    while i < 10 or R.have_waiting()[0]:
        time.sleep(0)
        R.do_serve_request(spin=True)
        print(R.get_status(key))
        print(R.get_response(key))
        print(R.have_waiting())
        i += 1


if __name__ == '__main__':
    key = transactor.random_key(20)
    s1 = lambda: server(key)
    c1 = lambda: client(key)
    s = threading.Thread(target=s1)
    c = threading.Thread(target=c1)
    c.start()
    s.start()
