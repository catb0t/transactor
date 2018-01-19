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
            return self._requests.push(req, want_nice=nice)

# post-work (epilogue)

    def impl_ul_get_descr(self, uuid, keep=False):
        res = self._descrs.get(uuid, None)

        if res is None:
            return None
        if keep:
            return res
        with self.lock:
            del self._descrs[uuid]
        return res

    def impl_get_descr(self, uuid, spin=False, keep=False):
        if not self.impl_have_own_uuid(uuid):
            return False
        descr = self.impl_ul_get_descr(uuid, keep=keep)
        if descr is not None:
            return descr
        if descr is None and not spin:
            return None

        while descr is None:
            time.sleep(0)  # yield on this thread
            descr = self.impl_ul_get_descr(uuid, keep=keep)
        return descr

    def impl_ul_get_response(self, uuid, keep=False):
        resp = self._responses.get(uuid, None)

        if resp is None:
            return None
        if keep:
            return resp
        with self.lock:
            del self._responses[uuid]
        return resp

    def impl_get_response(self, uuid, spin=False, keep=False):
        if not self.impl_have_own_uuid(uuid):
            return False

        resp = self.impl_ul_get_response(uuid, keep=keep)
        if resp is not None:
            return resp
        if resp is None and not spin:
            return None

        while resp is None:
            time.sleep(0)
            resp = self.impl_ul_get_response(keep=keep)
        return resp

# "background" API

    def impl_have_own_uuid(self, uuid):
        return uuid in self._known_uuids

# arbiter introduction

    def impl_pop_request(self, spin=False, keep=False):
        # top (right side) of the deque
        now = microtime()
        reqfun = self._requests.__getattribute__( ("pop", "peek")[keep] )
        req = reqfun()
        return req, now

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

    def impl_do_serve_request(self, metadata, func):
        if (
            metadata is None
            or "request" not in metadata
            or metadata["request"] is None
        ):
            return None, None
        uuid = metadata["request"].get("uuid", None)
        time.sleep(0)
        # do something with req
        start = microtime()
        res = {}
        status = 0
        try:
            res, status = func(metadata)
        except BaseException as e:
            res, status = e, -1
        finally:
            end = microtime()
            self.impl_set_response(uuid, res)
            descr = {
                "uuid": uuid,
                "status": status,
                "time": {
                  "issued": metadata["issued"],
                  "start": start,
                  "end":   end,
                }
            }
            self.impl_set_descr(descr)
            return res, descr

    def do_serve_request(self, func=lambda k: (k["request"]["dbname"], 200), spin=False, keep=False):
        data, iat = self.impl_pop_request(spin=spin)
        req, nice = data
        all_metadata = {
          "request": req,
          "nice": nice,
          "issued": iat
        }
        return self.impl_do_serve_request(all_metadata, func)


class write_clerk(request_clerk):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

# user API

    def register_write(self, req, prefunc=lambda x: x):
        return self.impl_register_request(req, prefunc=prefunc)  # stub

    def get_status(self, uuid, **kwargs):
        return self.impl_get_descr(uuid, **kwargs)


class read_clerk(request_clerk):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

# user API

    def register_read(self, req, prefunc=lambda x: x):
        return self.impl_register_request(req, prefunc=prefunc)  # stub

    def get_response(self, uuid, **kwargs):
        return self.impl_get_response(uuid, **kwargs)

    def get_status(self, uuid, **kwargs):
        return self.impl_get_descr(uuid, **kwargs)
