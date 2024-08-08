# Copyright Olivia Stevens 2024

import enum
import threading
import time

from binascii import hexlify
from datetime import datetime as dt
from os import urandom
from pathlib import Path

from ._prioritydeque import priority_deque, priority


class RequestType:
    WRITE = 'write'
    READ = 'read'


class UnwaitedEmptyResult:
    result = None
    reason = "[Default reason]: _impl_get_* returned None and `spin` was set to False"
    reason_format = "[Default reason]: {} returned None and `spin` was set to False"

    def __init__(self, result=None, func=None, reason=None):
        self.result = result
        if func is not None:
            self.reason = self.reason_format.format(func)
        elif reason is not None:
            self.reason = reason


def random_key(keysize=64):
    return hexlify(urandom(keysize // 2)).decode("ascii")


class request_clerk():
    """
        Base class for request pool management objects.

        Technically an abstract base class as impl_do_serve_request requires
            self to have _process_read_request or _process_write_request
            defined.
    """

    @enum.unique
    class _field(enum.Enum):
        request, request_type, nice, uuid, status, time, issued, start, end, \
            target_db, read_func, write_func, STOP_ITERATION = range(13)

        def describe(self):
            return self.name

        def __repr__(self):
            return self.describe()

        def __str__(self):
            return self.describe()

        def __invert__(self):
            return self.describe()

    def __init__(self, clerk_type, db_files=(), db_path=None, field_enum=None):
        """
            Create a new request pool manager.
        """
        if not db_files:
            raise ValueError("Must be non-empty: db_files")
        self.db_files = db_files

        # whether we can read or write but not both
        if clerk_type not in (RequestType.READ, RequestType.WRITE):
            raise ValueError('clerk_type must be in [RequestType.READ, RequestType.WRITE]')

        self.clerk_type = clerk_type
        # read and write request pool
        self._requests  = priority_deque()
        # impl detail, temp record of recent uuids
        self._known_uuids = set()
        # used only by read subclass; dict: uuid -> data
        self._responses = dict()
        # descriptions; dict: uuid -> rank, ok?, completed time
        self._descrs    = dict()
        # experimental
        self._journal   = ()
        # all self properties should be locked
        self.lock       = threading.Lock()
        if field_enum is not None:
            self.fields = field_enum
        else:
            self.fields = self._field

        if db_path is None:
            self.db_path = Path('.')

    def _check_request_type_matches(self, request_type, func_name):
        if not request_type == self.clerk_type or request_type not in (RequestType.READ, RequestType.WRITE):
            raise ValueError(f'{func_name}: request type must match clerk type: {request_type} is not {self.clerk_type}')

# "public" API

# pre-work (introduction)

    def impl_register_request(self, req, request_type, prefunc=lambda x: x):
        """
            params: req (a dict) and prefunc (a function x -> g)
            retval: None
            raises: KeyError if req is missing "uuid" or "nice" keys
            purity: relative

            Implementation detail to register a request in the pool.

            req[uuid] is registered as a known uuid.
            req is pushed to the end of the request pool based on its rank
        """
        self._check_request_type_matches(request_type, self.impl_register_request.__name__)

        req[~self.fields.request_type] = request_type
        nice = priority(req.get(~self.fields.nice, priority.undef))
        req  = prefunc(req)
        with self.lock:
            self._known_uuids.add( req[~self.fields.uuid] )
            return self._requests.push(req, want_nice=nice)

        return req

# post-work (epilogue)

    def impl_get_unload_descr(self, uuid, keep=False):
        with self.lock:
            res = self._descrs.get(uuid, None)

        time.sleep(0)
        if res is None:
            return None
        if keep:
            return res
        with self.lock:
            del self._descrs[uuid]
        return res

    def get_unload_descr(self, uuid, spin=False, keep=False):
        if not self.have_own_uuid(uuid):
            return False
        descr = self.impl_get_unload_descr(uuid, keep=keep)
        if descr is not None:
            return descr
        if descr is None and not spin:
            return UnwaitedEmptyResult(func=self.impl_get_unload_descr.__name__)

        while descr is None:
            time.sleep(0)  # yield on this thread
            descr = self.impl_get_unload_descr(uuid, keep=keep)
        return descr

    def impl_get_unload_response(self, uuid, keep=False):
        with self.lock:
            resp = self._responses.get(uuid, None)

        time.sleep(0)
        if resp is None:
            return None
        if keep:
            return resp
        with self.lock:
            del self._responses[uuid]
        return resp

    def get_unload_response(self, uuid, spin=False, keep=False):
        if not self.have_own_uuid(uuid):
            return False

        resp = self.impl_get_unload_response(uuid, keep=keep)
        if resp is not None:
            return resp
        if resp is None and not spin:
            return UnwaitedEmptyResult(func=self.impl_get_unload_response.__name__)

        while resp is None:
            time.sleep(0)
            resp = self.impl_get_unload_response(uuid, keep=keep)
        return resp

# "background" API

    def have_own_uuid(self, uuid):
        with self.lock:
            return uuid in self._known_uuids

# arbiter introduction

    def pop_request(self, spin=False, keep=False):
        # top (right side) of the deque
        now = dt.now()
        with self.lock:
            reqfun = self._requests.__getattribute__( ("pop", "peek")[keep] )
            req = reqfun()
        return req, now

    def have_waiting(self):
        with self.lock:
            return self._requests.peek()

# arbiter epilogue
    def get_descr(self, uuid, **kwargs):
        with self.lock:
            return self.get_unload_descr(uuid, **kwargs)

    def set_descr(self, descr):
        with self.lock:
            self._descrs[ descr[~self.fields.uuid] ] = descr

    def set_response(self, uuid, data):
        with self.lock:
            self._responses[uuid] = data

# arbiter stub

    def impl_do_serve_request(self, metadata, after_serve_func):
        from pprint import pprint

        if isinstance(metadata, UnwaitedEmptyResult):
            return None, metadata

        if (
            ~self.fields.request not in metadata
            or metadata[~self.fields.request] is None
        ):
            return None, None
            # raise ValueError(f"Request metadata is not valid: {repr(metadata)}")

        self._check_request_type_matches(metadata[~self.fields.request_type], self.impl_do_serve_request.__name__)

        uuid = metadata[~self.fields.request][~self.fields.uuid]
        request_type = metadata[~self.fields.request_type]

        # do something with req
        time.sleep(0)
        print(f"\nRequest metadata for <{request_type} {uuid}>")
        pprint(metadata)
        print(f"</{request_type} {uuid}>")

        if request_type == RequestType.WRITE:
            self._process_write_request(metadata)
        elif request_type == RequestType.READ:
            self._process_read_request(metadata)

        start = dt.now()
        res = {}
        status = 0
        try:
            res, status = after_serve_func(metadata)
        except BaseException as e:
            res, status = e, -1
        finally:
            end = dt.now()
            self.set_response(uuid, res)
            descr = {
                ~self.fields.uuid: uuid,
                ~self.fields.status: status,
                ~self.fields.time: {
                  ~self.fields.issued: metadata[~self.fields.issued],
                  ~self.fields.start: start,
                  ~self.fields.end:   end,
                }
            }
            self.set_descr(descr)
        return res, descr

    def do_serve_request(self, spin=False, keep=False, func=None):
        if func is None:
            def func(k):
                return (
                    k[~request_clerk._field.request]
                    [~request_clerk._field.target_db],
                    200
                )
        data, issued_at = self.pop_request(spin=spin, keep=keep)
        req, nice = data
        all_metadata = {
          ~self.fields.request: req,
          ~self.fields.nice: nice,
          ~self.fields.issued: issued_at
        }
        return self.impl_do_serve_request(all_metadata, func)


class write_clerk(request_clerk):

    def __init__(self, clerk_type, db_path, db_files, field_enum=None):
        super().__init__(clerk_type, db_files=db_files, db_path=db_path, field_enum=field_enum)

    def _process_write_request(self):
        self._check_request_type_matches(RequestType.WRITE, self._process_write_request.__name__)


# user API

    def register_write(self, req, prefunc=lambda x: x):
        return self.impl_register_request(req, RequestType.WRITE, prefunc=prefunc)  # stub


class read_clerk(request_clerk):

    def __init__(self, clerk_type, db_path, db_files, field_enum=None):
        super().__init__(clerk_type, db_files=db_files, db_path=db_path, field_enum=field_enum)

# user API

    def register_read(self, req, prefunc=lambda x: x):
        return self.impl_register_request(req, RequestType.READ, prefunc=prefunc)  # stub

    def get_response(self, uuid, **kwargs):
        return self.get_unload_response(uuid, **kwargs)
