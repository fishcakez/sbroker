squeue
======
Sojourn queue - queue library using sojourn time based active queue management.

Introduction
------------
A subset of the `squeue` API is very similar to the `queue` module's API. There
are two differences. Firstly when `{value, Item}` is returned by `queue`,
`squeue` returns `{SojournTime, Item}`, where `SojournTime`
(`non_neg_integer()`) is the sojourn time of the item (or length of time an item
waited in the queue). Secondly `out/1`, `out_r/1`, `drop/1` and
`drop_r/1` return `{Result, Drops, Queue} or `{Drops, Queue}`, where the extra
term `Drops` is a list of 2-tuples containing items (and their sojourn times)
dropped by the queue management algorithm.

`squeue` also provides an optional first argument to all `queue`
functions: `Time`. This argument is of type `non_neg_integer()` and sets
the current time of the queue, if `Time` is greater than (or equal) to
the queue's previous time. When the `Time` argument is included items
may be dropped by the queues management algorithm. The dropped items
will be included in the return value when the queue itself is also
returned. The dropped items are a list of the form: `[{SojournTime, Item}]`,
which is ordered with them item with the greatest `SojournTime` (i.e. the
oldest) at the head.


`squeue` includes 4 queue management algorithms (in order of complexity):
* `squeue_naive`
* `squeue_timeout`
* `squeue_codel`
* `squeue_codel_timeout`

squeue_naive
------------

`squeue_naive` does not do any queue management, will never drop
any items and is the default queue management backend. An empty `squeue`
"managed" by `squeue_naive` can be created in one of four ways (where `Time`
is the initial time, of type `non_neg_integer()`, and `Any` is any term and is
ignored):
```erlang
squeue:new().
squeue:new(Time).
squeue:new(squeue_naive, Any).
squeue:new(Time, squeue_naive, Any).
```

squeue_timeout
--------------
`squeue_timeout` will drop any items with a sojourn time greater than a
timeout value. Create an empty `squeue` managed by `squeue_timeout` (`Timeout`
is the timeout length, of type `pos_integer()`, and `Time` is the initial time,
of type `non_neg_integer()`):
```erlang
squeue:new(squeue_timeout, Timeout).
squeue:new(Time, squeue_timeout, Timeout).
```

squeue_codel
------------
`squeue_codel` will drop items using an implementation of the CoDel queue
management algorithm. Create an empty `squeue` managed by `squeue_codel` (where
`Target` is the target sojourn time, of type `pos_integer()`; `Interval`, the
initial interval between drops, of type `pos_integer()`; `Time`, the initial
time, of type `non_neg_integer()`):
```erlang
squeue:new(squeue_codel, {Target, Interval}).
squeue:new(Time, squeue_codel, {Target, Interval}).
```

squeue_codel_timeout
--------------------
`squeue_codel_timeout` combines `squeue_codel` and `squeue_timeout` so
that the CoDel algorithm is used with the addition that any items with a
sojourn time over the timeout are dequeued. The dequeuing of timed out
items will trigger the CoDel algorithm and so item with sojourn time
below the timeout may also be dropped if the backlog is significant. Create an
empty `squeue` managed by `squeue_codel_timeout` (where `Target` is the target
sojourn time, of type `pos_integer()`; `Interval`, the initial interval between
drops, of type `pos_integer()`; `Timeout` is the timeout length, of type
`pos_integer()`; `Time`, the initial time, of type `non_neg_integer()`):
```erlang
squeue:new(squeue_codel_timeout, {Target, Interval, Timeout}).
squeue:new(Time, squeue_codel_timeout, {Target, Interval, Timeout}).
```
`Timeout` must be greater than `Target`, if it is not then the CoDel
alogirthm would not be applied.

Custom Management
-----------------
It is planned to finalise and document the `squeue` behaviour to allow custom
queue management modules. Note that the API will likely only be suitable
for use of queue management algorithm that rely on the sojourn time of
the head (oldest item) in the queue, and do not depend on queue size or
the position of certain items.

Additional API
--------------
Retrieve the current time of a queue (with `Queue` as the queue):
```erlang
squeue:time(Queue)
```
Advance the current time of the queue (with `Time`, the new time; `Queue`,
the queue; `Drops`, the dropped items; `NQueue`, the new queue after
dropping items):
```erlang
{Drops, NQueue} = squeue:timeout(Time, Queue)
```
By combining the `queue`-like API described in the introduction and `timeout/2`
it is possible to add active queue management to a module currently using
`queue` with minimal refactoring.

Build
-----
Rebar builds:
```
rebar compile
```

Documentation
-------------
Rebar builds documentation:
```
rebar doc
```
Then visit `doc/index.html`.

Test
----
Rebar fetches test dependency and runs common test:
```
rebar get-deps compile ct -C rebar.test.config
```

License
-------
Dual BSD/GPL license

This is the license used by the CoDel authors in their example implementation.
It was chosen for `squeue` to honour their desired freedoms of use of CoDel.

Note when distributing beam files that a test only dependency (`PropEr`)
is licensed under GPL. This dependency is not fetched by rebar unless
`rebar.test.config` is specified as the config file, only files in `test/` use
it and if it is not present any test requiring it will be skipped.
