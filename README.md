# transactor

---

simple transactional scheduler and prioritising double ended queue for Python3

intended for threading applications but can be used in single-thread applications too

see `example.py`

TODO:
- [ ] make return tuple objects should be class instances or dictionaries for indexing that isn't like `[0][0]`

```
$ ./example.py
priority.airmail continue
priority.airmail continue
priority.normal continue
priority.normal continue
priority.low continue
priority.low continue
priority.undef STOPITER
('continue', <priority.airmail: 3>) 	 {'uuid': 'a3fbd68800', 'status': 200, 'time': {'issued': 1516381312397581, 'start': 1516381312397692, 'end': 1516381312397761}}
('STOPITER', <priority.undef: -1>) 	 {'uuid': 'dd1725b4b8', 'status': 200, 'time': {'issued': 1516381312460097, 'start': 1516381312460232, 'end': 1516381312460286}}
('continue', <priority.normal: 1>) 	 {'uuid': '1468610894', 'status': 200, 'time': {'issued': 1516381312418249, 'start': 1516381312418498, 'end': 1516381312418686}}
('continue', <priority.low: 0>) 	 {'uuid': 'a102c9f69b', 'status': 200, 'time': {'issued': 1516381312439099, 'start': 1516381312439258, 'end': 1516381312439333}}
('continue', <priority.normal: 1>) 	 {'uuid': 'd8324ba7c4', 'status': 200, 'time': {'issued': 1516381312428806, 'start': 1516381312428915, 'end': 1516381312428975}}
None 	 None
('continue', <priority.airmail: 3>) 	 {'uuid': '080a7298e0', 'status': 200, 'time': {'issued': 1516381312407878, 'start': 1516381312408001, 'end': 1516381312408064}}
('continue', <priority.low: 0>) 	 {'uuid': '9a246971e2', 'status': 200, 'time': {'issued': 1516381312449478, 'start': 1516381312449695, 'end': 1516381312449952}}
```