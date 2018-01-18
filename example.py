#!/usr/bin/env python3

import transactor
import threading
import time

R = transactor.read_clerk()


def client(key):
    while True:
        R.register_read({
          "uuid": key,
          "nice": transactor.priority.normal,
          "dbname": "users"
        })
        time.sleep(0)


def server(key):
    while True:
        R.do_serve_request(spin=True)
        print(R.get_status(key))
        print(R.get_response(key))
        time.sleep(0)


if __name__ == '__main__':
    key = transactor.random_key(20)
    s1 = lambda: server(key)
    c1 = lambda: client(key)
    s = threading.Thread(target=s1)
    c = threading.Thread(target=c1)
    s.start()
    # time.sleep(0)
    c.start()
