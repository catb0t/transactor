import enum


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


def get_maxlen(params, key):
    return params.get(key, DEFAULT_MAXLENS[key])


class priority_deque():
    '''
        Base class for priority deque objects.
    '''

    @staticmethod
    def default_nice_sorter(nices):
        return sorted(nices, key=lambda x: x.value, reverse=True)

    @staticmethod
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
        self, obj, want_nice=None, force=False,
        want_push_func=lambda q, o: q.appendleft(o),
        settle_push_func=lambda q, o: q.append(o)
    ):
        '''
            params: obj (an object)
                    want_nice ** (a priority; default: self.prty.normal)
                    force ** (a bool; default: false)
                    want_push_func ** (a function q, o -> None;
                        default: appendleft)
                    settle_push_func ** (a function q, o -> None;
                        default: append)
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
            If the preferred nice value want_nice is full and force=False,
                settle_push_func will be used to "settle for" a lower nice
                value.
            By default, this secondary function pushes to the top of the next
                lowest priority.

            If force=False, this method is not destructive; it will try to
                push on a deque in the pool which is not full.
            To force pushing an object into a specific priority even if they
                are full, set force=True.
        '''
        import time
        if want_nice is None:
            want_nice = self.prty.normal
        if force or self._can_push(want_nice):
            time.sleep(0)
            with self.lock:
                return want_push_func(self._pool[want_nice], obj), want_nice

        # start from the highest priority and go down
        nices = range(want_nice, priority.min()[0])
        for nice in nices:
            # nice != want_nice
            time.sleep(0)
            if self._can_push(nice):
                    with self.lock:
                        return settle_push_func(self._pool[nice], obj), nice

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
        import time
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
            with self.lock:
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
