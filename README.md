squeue
======
Sojourn queue - queue library using sojourn time based active queue management.

Introduction
------------
A subset of the `squeue` API is the `queue` module's API with one exception:
when `{value, Item}` is returned by `queue`, `squeue` returns
`{SojournTime, Item}`, where `SojournTime` (`non_neg_integer()`) is the
sojourn time of the item (or length of time an item waited in the queue).

`squeue` also provides an optional first argument to all `queue`
functions: `Time`. This argument is of type `non_neg_integer()` and sets
the current time of the queue, if `Time` is greater than (or equal) to
the queue's previous time. When the `Time` argument is included items
may be dropped by the queues management algorithm. The dropped items
will be included in the return value when the queue itself is also
returned. The dropped items are a list of the form: `[{SojournTime, Item}]`,
which is ordered with them item with the greatest `SojournTime` (i.e. the
oldest) at the head. If the `Time` argument is equal to the current time
of the queue no items are dropped.

`squeue` includes 3 queue management algorithms (in order of complexity):
* `squeue_naive`
* `squeue_timeout`
* `squeue_codel`

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
management algorithm. There are two notable variations. Firstly that items are
dropped when time advances, and not when dequeuing. Secondly that the time the
queue became slow, rather than the current time when a slow queue discovers it
is slow, is used when deciding when to a drop the first item after a period of
not dropping. Create an empty `squeue` managed by `squeue_codel` (where `Target`
is the target sojourn time, of type `pos_integer()`; `Interval`, the initial
interval between drops, of type `pos_integer()`; `Time`, the initial time, of
type `non_neg_integer()`):
```erlang
squeue:new(squeue_codel, {Target, Interval}).
squeue:new(Time, squeue_codel, {Target, Interval}).
```
Custom Management
-----------------
It is planned to finalise and document the `squeue` behaviour to allow custom
queue management modules.


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
