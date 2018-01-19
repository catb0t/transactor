import time
from prioritydeque import priority_deque, priority


def random_key(keysize=64):
    from os       import urandom
    from binascii import hexlify
    return hexlify(urandom(keysize // 2)).decode("ascii")


def microtime():
    return round( (10 ** 6) * time.time() )


class request_clerk():
    '''
        Base class for request pool management objects.
    '''

    def __init__(self, *args, **kwargs):
        '''
            Create a new request pool manager.
        '''
        import threading
        # read and write request pool
        self._requests    = priority_deque()
        # impl detail, temp record of recent uuids
        self._known_uuids = set()
        # used only by read subclass; dict: uuid -> data
        self._responses   = dict()
        # descriptions; dict: uuid -> rank, ok?, completed time
        self._descrs      = dict()
        self.lock         = threading.Lock()
# "public" API

# pre-work (introduction)

    def impl_register_request(self, req, prefunc=lambda x: x):
        '''
            params: req (a dict) and prefunc (a function x -> g)
            retval: None
            raises: KeyError if req is missing "uuid" or "nice" keys
            purity: relative

            Implementation detail to register a request in the pool.

            req[uuid] is registered as a known uuid.
            req is pushed to the end of the request pool based on its rank
        '''
        req  = prefunc(req)
        nice = priority(req.get("nice", priority.undef))
        with self.lock:
            self._known_uuids.add( req["uuid"] )
            return self._requests.push(req, nice=nice)

# post-work (epilogue)

    def impl_ul_get_descr(self, uuid):
        res = self._descrs.get(uuid, None)

        if res is None:
            return None
        with self.lock:
            del self._descrs[uuid]
        return res

    def impl_get_descr(self, uuid, spin=False):
        if not self.impl_have_own_uuid(uuid):
            return False
        descr = self.impl_ul_get_descr(uuid)
        if descr is not None:
            return descr
        if descr is None and not spin:
            return None

        while descr is None:
            time.sleep(0)  # yield on this thread
            descr = self.impl_ul_get_descr(uuid)
        return descr

    def impl_ul_get_response(self, uuid):
        resp = self._responses.get(uuid, None)

        if resp is None:
            return None
        with self.lock:
            del self._responses[uuid]
        return resp

    def impl_get_response(self, uuid, spin=False):
        if not self.impl_have_own_uuid(uuid):
            return False

        resp = self.impl_ul_get_response(uuid)
        if resp is not None:
            return resp
        if resp is None and not spin:
            return None

        while resp is None:
            time.sleep(0)
            resp = self.impl_ul_get_response()
        return resp

# "background" API

    def impl_have_own_uuid(self, uuid):
        return uuid in self._known_uuids

# arbiter introduction

    def impl_pop_request(self):
        # top (right side) of the deque
        return self._requests.pop(), microtime()

    def have_waiting(self):
        return self._requests.peek()

# arbiter epilogue

    def impl_set_descr(self, descr):
        with self.lock:
            self._descrs[ descr["uuid"] ] = descr

    def impl_set_response(self, uuid, data):
        with self.lock:
            self._responses[uuid] = data

# arbiter stub

    def impl_do_serve_request(self, req, iat, func):
        uuid = req["uuid"]
        time.sleep(0)
        # do something with req
        res = func(req)
        self.impl_set_response(uuid, res)
        self.impl_set_descr( {"uuid": uuid, "status": 200, "time": iat} )

    def do_serve_request(self, func=lambda k: k["dbname"], spin=False):
        md, iat = self.impl_pop_request()
        req, nice = md
        if req is None:
            if not spin:
                return None
            while req is None:
                time.sleep(0)
                md, iat = self.impl_pop_request()
                req, nice = md
        self.impl_do_serve_request(req, iat, func)


class write_clerk(request_clerk):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

# user API

    def register_write(self, req, prefunc=lambda x: x):
        return self.impl_register_request(req, prefunc=prefunc)  # stub

    def get_status(self, uuid):
        return self.impl_get_descr(uuid)


class read_clerk(request_clerk):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

# user API

    def register_read(self, req, prefunc=lambda x: x):
        return self.impl_register_request(req, prefunc=prefunc)  # stub

    def get_response(self, uuid):
        return self.impl_get_response(uuid)

    def get_status(self, uuid):
        return self.impl_get_descr(uuid)
