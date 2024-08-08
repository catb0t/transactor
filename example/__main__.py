#!/usr/bin/env python3
# Copyright Olivia Stevens 2024

import threading
import time
import os
import random

from pathlib import Path
from pprint import pprint

import transactor

db_path = Path(os.path.dirname(os.path.realpath(__file__))) / "db"
R = transactor.read_clerk(db_path=db_path)


def client():
    keys = ()
    for i in range(3):
        keys += (transactor.random_key(10),)
        nice = random.choice(list(transactor.priority))

        def read_func(db):
            return db['cat']

        read_info = {
            ~R.fields.uuid: keys[i],
            ~R.fields.nice: nice,
            ~R.fields.target_db: "users",
            ~R.fields.read_func: read_func,
            ~R.fields.STOP_ITERATION: "STOPITER"
                if nice < transactor.priority.low else "continue"  # noqa
        }
        regd = R.register_read(read_info)
        print(f"\nClient registered <read {keys[-1]}>")
        pprint(regd)
        print(f"</read {keys[-1]}>")
        time.sleep(0)

    print("Sleep 15")
    time.sleep(15)
    # come back later
    for key in keys:
        print("Client:", R.get_response(key), "\t", R.get_descr(key))


def server():
    i = 0
    while i < 10 or R.have_waiting()[0]:
        time.sleep(.01)

        def after_serve_func(x):
            datas = (
                x[~R.fields.request][~R.fields.STOP_ITERATION],
                x[~R.fields.request][~R.fields.nice]
            )
            print("Server:", *datas)
            return datas, 200

        # datas (arbiter return value) is returned as 'result'
        result, _descr = R.do_serve_request(
            spin=True, after_serve_func=after_serve_func
        )
        if result and result[0] == "STOPITER":
            break
        i += 1


if __name__ == '__main__':
    s = threading.Thread(target=server)
    c = threading.Thread(target=client)
    c.start()
    s.start()
