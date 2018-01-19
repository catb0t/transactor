#!/usr/bin/env python3

import transactor
import threading
import time

R = transactor.read_clerk()


def client():
    for i in range(11):
        key = transactor.random_key(20)
        R.register_read({
          "uuid": key,
          "nice": transactor.priority.normal,
          "dbname": "users"
        })
        time.sleep(.1)
        print(R.get_response(key), R.get_status(key))


def server():
    i = 0
    while i < 10 or R.have_waiting()[0]:
        time.sleep(.01)
        R.do_serve_request(spin=True)
        i += 1


if __name__ == '__main__':
    s = threading.Thread(target=server)
    c = threading.Thread(target=client)
    c.start()
    s.start()
