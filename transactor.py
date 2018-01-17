from collections import deque
import time


def microtime():
    return round( (10 ** 6) * time.time() )


def random_key(keysize=64):
    from os       import urandom
    from binascii import hexlify
    return hexlify(urandom(keysize // 2)).decode("ascii")


def _static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate


@_static_vars(index=0)
def keyfun_r(keysize, alpha=__import__("string").ascii_lowercase):
    '''simple reentrant predictable key generator'''
    if not keysize:
        keyfun_r.index = 0
        return

    if keyfun_r.index + keysize >= len(alpha):
        slc = alpha[keyfun_r.index:]
        keyfun_r.index = 0
    else:
        slc = alpha[keyfun_r.index : keyfun_r.index + keysize]
        keyfun_r.index += keysize

    return slc


class hashable_dict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __hash__(self):
        keys = list(self.keys())
        vals = list(self.values())
        hashes = ()
        for i in range(len(keys)):
            hashes += ( ( hash(keys[i]), hash(vals[i]) ), )

        hashstr = "".join( ["".join([str(n) for n in p]) for p in hashes] )
        return hash(hashstr)


class request_clerk():

    def __init__(self, *args, **kwargs):
        # read and write request pool
        self._requests  = deque()
        self._known_uuids = set()
        # used only by read subclass; dict: uuid -> data
        self._responses = dict()
        # status codes etc; dict: uuid -> rank, ok?, completed time
        self._descrs    = dict()

# "public" API

# pre-work (introduction)

    def impl_register_request(self, req, prefunc=lambda x: x):
        self._known_uuids.add(req["uuid"])
        self._requests.appendleft(req)

# post-work (epilogue)

    def impl_ul_get_descr(self, uuid):
        res = self._descrs.get(uuid, None)

        if res is None:
            return None
        del self._descrs[uuid]
        return res

    def impl_get_descr(self, uuid, spin=False):
        if not self.impl_have_own_uuid(uuid):
            return False
        descr = self.impl_view_descr(uuid)
        if descr is not None:
            return descr
        if descr is None and not spin:
            return None

        while descr is None:
            time.sleep(0)  # yield on this thread
            descr = self.impl_view_descr(uuid)
        return descr

    def impl_ul_get_response(self, uuid):
        resp = self._responses.get(uuid, None)

        if resp is None:
            return None
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
        return self._requests.pop(), microtime()

# arbiter epilogue

    def impl_record_descr(self, res):
        self.descrs[ res["uuid"] ] = res


class write_clerk(request_clerk):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

# user API

    def register_write(self, req):
        self.impl_register_request(req)  # stub


class read_clerk(request_clerk):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

# user API

    def register_read(self, req):
        self.impl_register_request(req)  # stub

    def get_response(self, uuid):
        self._impl_get_response(uuid)
