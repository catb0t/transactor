#!/usr/bin/env python3

import transactor
import threading
import time

R = transactor.read_clerk()


def client():
    import random
    keys = ()
    for i in range(8):
        keys += transactor.random_key(10),
        nice = random.choice(list(transactor.priority))
        R.register_read({
          "uuid": keys[i],
          "nice": nice,
          "dbname": "users",
          "STOP_ITERATION": "STOPITER"
                            if nice < transactor.priority.normal
                            else "continue"
        })
        time.sleep(0)
    time.sleep(.5)
    # come back later
    for key in keys:
        print(R.get_response(key), "\t", R.get_status(key))


def server():
    i = 0
    while i < 10 or R.have_waiting()[0]:
        time.sleep(.01)

        def arbiter(x):
            print(x["request"]["nice"])
            return (x["request"]["STOP_ITERATION"], x["request"]["nice"]), 200
        res = R.do_serve_request(spin=True, func=arbiter)
        if "STOPITER" == res[0]:
            break
        i += 1


if __name__ == '__main__':
    s = threading.Thread(target=server)
    c = threading.Thread(target=client)
    c.start()
    s.start()
