import enum
import time


@enum.unique
class priority(enum.IntEnum):
    undef, low, normal, high, airmail = range(-1, 4)

    def describe(self):
        return self.value, self.name

    @classmethod
    def min(cls):
        return min(
            [e.describe() for e in list(cls)],
            key=lambda x: x[0]
        )

    @classmethod
    def max(cls):
        return max(
            [e.describe() for e in list(cls)],
            key=lambda x: x[0]
        )


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


def get_maxlen(params, key):
    return params.get(key, DEFAULT_MAXLENS[key])


class priority_deque():
    '''
        Base class for priority deque objects.
    '''

    @staticmethod
    def default_nice_sorter(nices):
        return sorted(nices, key=lambda x: x.value, reverse=True)

    def random_nice_sorter(nices):
        import random
        return random.shuffle(nices, len(nices))

    def __init__(self, *args, **kwargs):
        '''
            params: priority_enum ** (an alternative enum class with the same
                        member names as the priority class)
            retval: a blank priority_deque
            raises: AttributeError if priority_enum has an unexpected set
                        of names
            purity: yes

            Create an empty priority deque.
        '''
        import threading
        from collections import deque
        self.prty = priority
        if "priority_enum" in kwargs:
            self.prty = kwargs["priority_enum"]
        self._pool = {
            self.prty.undef:
                deque(maxlen=get_maxlen(kwargs, self.prty.undef)),
            self.prty.low:
                deque(maxlen=get_maxlen(kwargs, self.prty.low)),
            self.prty.normal:
                deque(maxlen=get_maxlen(kwargs, self.prty.normal)),
            self.prty.high:
                deque(maxlen=get_maxlen(kwargs, self.prty.high)),
            self.prty.airmail:
                deque(maxlen=get_maxlen(kwargs, self.prty.airmail))
        }
        self.lock = threading.Lock()

    def push(
        self, obj, nice=None, force=False,
        push_func=lambda q, o: q.appendleft(o),
    ):
        '''
            params: obj (an object)
                    nice ** (a priority; default: self.prty.normal)
                    force ** (a bool; default: false)
                    push_func ** (a function q, o -> None; default: appendleft)
            retval: None (a NoneType)
                    nice (a priority; the priority that obj ended up with)
            raises: KeyError if nice is not a key in self.prty (that is, it
                        is not a key in self._pool)
            purity: relative

            Add a new entry to the pool, with the maximum priority of nice.
            The entry may end up with a lower priority because all the other
                deques were full.

            obj can be pushed to the top (right side) of a deque by specifying
                push_func like (lambda q, o: q.append(o)).

            If force is false, this method is not destructive; it will try to
                push on a deque in the pool which is not full.
            To force pushing an object into a specific priority even if they
                are full, set force=True.
        '''
        if nice is None:
            nice = self.prty.normal
        if force or self._can_push(nice):
            time.sleep(0)
            with self.lock:
                return push_func(self._pool[nice], obj), nice

        # start from the highest priority and go down
        nices = range(nice, priority.min()[0])
        for nice in nices:
            time.sleep(0)
            if self._can_push(nice):
                with self.lock:
                    return push_func(self._pool[nice], obj), nice

    def pop(
        self, force_nice=(False, None),
        nice_sorter=None, pop_func=lambda q: q.pop()
    ):
        '''
            params: force_nice ** (a pair<bool, priority>;
                        default: (Force, None))
                    nice_sorter ** (a function n -> s;
                        default: priority_deque.default_nice_sorter)
                    pop_func ** (a function q -> o; default: pop)
            retval: obj (an object)
                    nice (a priority; the priority obj had)
            raises: KeyError if force_nice isn't long enough
                    KeyError if force_nice[1] is not a key in self.prty
            purity: relative

            Remove an entry from the pool.
            By default, looks for the highest-priority items first.
            The priority of the resulting object is returned alongside it.
            If no object was found, an object of None and a priority of None
                are returned.

            The deques are sorted by nice_sorter, and the highest-priority non-
                empty deque is popped from with pop_func.
            To look for lower priorities first, use a function which does not
                reverse-sort the priority list.
            To use a random priority, use self.random_nice_sorter
            To pop from a specific priority, use force_nice=(True, nice).
            This will return an object or None (if the priority was empty) and
                the provided priority.
        '''
        if nice_sorter is None:
            nice_sorter = self.default_nice_sorter
        if force_nice[0]:
            time.sleep(0)
            with self.lock:
                return pop_func(self._pool[ force_nice[1] ]), force_nice[1]

        nices = self._sort_pool(nice_sorter)
        for nice in nices:
            time.sleep(0)
            dq = self._pool[nice]
            if len(dq):
                with self.lock:
                    return pop_func(dq), nice
        return None, None

    def peek(
        self, force_nice=(False, None),
        nice_sorter=None, peek_func=lambda q: q[-1]
    ):
        '''
            params: force_nice ** (a pair<bool, priority>;
                        default: (Force, None))
                    nice_sorter ** (a function n -> s;
                        default: priority_deque.default_nice_sorter)
                    pop_func ** (a function q -> o;
                        default: lambda q: q[-1])
            retval: obj (an object)
                    nice (a priority; the priority obj has)
            raises: KeyError if force_nice isn't long enough
                    KeyError if force_nice[1] is not a key in self.prty
            purity: relative

            View an entry in the pool.
        '''
        if nice_sorter is None:
            nice_sorter = self.default_nice_sorter
        if force_nice[0]:
            with self.lock:
                return peek_func(self._pool[ force_nice[1] ]), force_nice[1]
        return self.pop(nice_sorter=nice_sorter, pop_func=peek_func)

    def clear1(self, nice):
        dq = self._pool[nice].copy()
        with self.lock:
            self._pool[nice].clear()
        return dq

    def clear(self):
        pool = self._pool.copy()
        for nice in self.prty:
            self._pool[nice].clear()
        return pool

    def _sort_pool(self, nice_sorter=default_nice_sorter):
        return nice_sorter( self.prty )

    def _can_push(self, nice):
        if self._pool[nice].maxlen is None:
            return True
        return len( self._pool[nice] ) < self._pool[nice].maxlen

    def __repr__(self):
        return repr(self._pool)


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
