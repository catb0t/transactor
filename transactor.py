from collections import deque
import enum
import time


class priority(enum.IntEnum):
    undef  = 0
    low    = 1
    normal = 2
    high   = 3
    airmail = 4


DEFAULT_MAXLENS = {
    priority.undef:   None,
    priority.low:     None,
    priority.normal:  None,
    priority.high:    50,
    priority.airmail: 10
}


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


def get_maxlen(params, key):
    return params.get(key, DEFAULT_MAXLENS[key])


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


class deque_pool():
    @staticmethod
    def default_nice_sorter(nices):
        return sorted(nices, key=lambda x: x.value, reverse=True)

    def __init__(self, *args, **kwargs):
        self.pool = {
            priority.undef:
                deque(maxlen=get_maxlen(kwargs, priority.undef)),
            priority.low:
                deque(maxlen=get_maxlen(kwargs, priority.low)),
            priority.normal:
                deque(maxlen=get_maxlen(kwargs, priority.normal)),
            priority.high:
                deque(maxlen=get_maxlen(kwargs, priority.high)),
            priority.airmail:
                deque(maxlen=get_maxlen(kwargs, priority.airmail))
        }

    def push(
        self, obj,
        nice=priority.normal,
        push_func=lambda q, o: q.appendleft(o)
    ):
        if self._can_push(nice):
            push_func(self.pool[nice], obj)
            return nice

        while not self._can_push(nice) and (nice >= priority.undef):
            nice -= 1
        push_func(self.pool[nice], obj)
        return nice

    def pop(self):
        return self.pop_nice()

    def pop_nice(
        self,
        nice_sorter=default_nice_sorter,
        pop_func=lambda q: q.pop()
    ):
        nices = self.sort_pool(nice_sorter)
        for ni in nices:
            dq = self.pool[ni]
            if len(dq):
                return pop_func(dq)
        return None

    def sort_pool(self, nice_sorter=default_nice_sorter):
        return nice_sorter( self.pool.keys() )

    def pop_random(self):
        import random
        return self.sort_pool( lambda x: random.sample(x, len(x)) )

    def _can_push(self, nice):
        if None is not self.pool[nice].maxlen:
            return len( self.pool[nice] ) < self.pool[nice].maxlen
        return True


class request_clerk():

    def __init__(self, *args, **kwargs):
        '''
            Base class for request pool management objects.
        '''
        # read and write request pool (list?)
        self._requests  = deque_pool()
        # impl detail, temp record of recent uuids
        self._known_uuids = set()
        # used only by read subclass; dict: uuid -> data
        self._responses = dict()
        # descriptions; dict: uuid -> rank, ok?, completed time
        self._descrs    = dict()

# "public" API

# pre-work (introduction)

    def impl_register_request(self, req, prefunc=lambda x: x):
        '''
            Arguments:  req (a dict) and prefunc (a function x -> g)
            Returns:    None
            Throws:     KeyError if req is missing
                        "uuid", "nice" or "rank" keys
            Effects:    modifies _known_uuids and _requests, extending them

            Implementation detail to register a request in the pool.

            req[uuid] is registered as a known uuid.
            req is pushed to the end of the request pool based on its rank
        '''
        # TODO: rank
        req = prefunc(req)
        rank = req.get("rank", req.get("nice", 20))
        self._known_uuids.add( req["uuid"] )
        self._requests.append_left_ranked(req, rank)

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
        # top (right side) of the deque
        return self._requests.pop(), microtime()

# arbiter impl

    def a(self):
        pass

# arbiter epilogue

    def impl_set_descr(self, descr):
        self._descrs[ descr["uuid"] ] = descr

    def impl_set_response(self, uuid, data):
        self._responses[uuid] = data


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
