sbroker
=======

Sojourn Broker - process broker for matchmaking between two groups of processes
using sojourn time based active queue management to prevent congestion.

Introduction
------------

`sbroker` is an experiment at an alternative to pooling. The philosophy
is slightly different to traditional erlang pooling approaches as an
`sbroker` process treates both sides (clients and workers) identically
so it is more like a client-client relationship. Conceptual this is
slightly different as both groups are exchanging themselves to gain a
process from the other group. Whereas in a worker pool model the clients
contact the pool seeking a worker. This means that workers contacting an
`sbroker` should always "want" work, just as clients always "want" a
worker for work.

In terms of the traditional client-worker architecture it can be helpful for
workers to call `ask/1` as they are offering their time, and clients to call
`bid/1` as they are trying to obtain a workers time. However `sbroker` would
work exactly the same the reverse way around.

`sbroker` provides a simple interface to match processes. One party
calls `sbroker:bid/1` and the other party `sbroker:ask/1`. If a match
is found both return `{settled, Ref, Pid, SojournTime}`, where `SojournTime` is
the time spent in milliseconds waiting for a match (one will have a time
of 0), `Pid` is the other process in the match and `Ref` is the transaction
reference. If no match is found, returns `{dropped, SojournTime}`.

Processes calling `sbroker:bid/1` are only matched with a process calling
`sbroker:ask/1` and vice versa.

Example
-------

```erlang
{ok, Broker} = sbroker:start_link(),
Pid = spawn_link(fun() -> sbroker:ask(Broker) end),
{settled, _Ref, Pid, _SojournTime} = sbroker:bid(Broker).
```

Usage
-----

`sbroker` provides configurable queues defined by `sbroker:queue_spec()`s. A
`queue_spec` takes the form:
```erlang
{Module, Args, Out, Size, Drop}
```
`Module` is an `squeue` callback module to handle active queue
management. The following modules are possible: `squeue_naive`,
`squeue_timeout`, `squeue_codel` and `squeue_codel_timeout`.
`Args` is the argument passed to the callback module. Information about
the different backends and their arguments are avaliable in the
documentation.

`Out` sets the dequeue function, either the atom `out` (FIFO) or the
atom `out_r` (LIFO).

`Size` is the maximum size of the queue. Should the queue go above this
size a process is dropped. The dropping strategy is determined by
`Drop`, which is either the atom `drop` (head drop) or the atom `drop_r`
(tail drop).

An `sbroker` is started using `sbroker:start_link/0,1,3,4`:
```erlang
sbroker:start_link(BiddingSpec, AskingSpec, Interval).
```
`BiddingSpec` is the `queue_spec` for the queue containing processes calling
`bid/1`. The queue is referred to as the bidding queue. Similarly `AskingSpec`
is the `queue_spec` for the queue contaning processes calling `ask/1`.

`Interval` is the interval in milliseconds that an `sbroker` is
polled to apply timeout queue management. Note that timeout queue
management can occur on every enqueue and dequeue, and is not reliant on
the `Interval`. Setting a suitable interval ensures that active queue
management can occur if no processes are queued or dequeued for a period
of time.

Asynchronous versions of `bid/1` and `ask/1` are avaliable as
`async_bid/1` and `async_ask/1`. On a successful match the following
message is sent:
```erlang
{Ref, {settled, Ref, Pid, SojournTime}}
```
Where `Ref` is the return value of `async_bid/1` or `async_ask/1`. If a
match is not found:
```erlang
{Ref, {dropped, SojournTime}}
```

Asynchronous requests can be cancelled with `cancel/2`:

```erlang
{ok, Broker} = sbroker:start_link().
Ref = sbroker:async_bid(Broker).
ok = sbroker:cancel(Broker, Ref).
```
To help prevent race conditions when using asynchronous requests the
message to the `async_ask/1` or `ask/1` process is always sent before
the message to the `async_bid/1` or `bid/1` process. Therefore if the
initial message between the two groups always flows in one direction,
it may be beneficial for the receiver of that message to call
`async_ask/1` or `ask/1`, and the sender to call `async_bid/1 or `bid/1`.

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

Roadmap
-------

* Implement a pool that resizes based on feedback from sbroker.

License
-------
Dual BSD/GPL license

This is the license used by the CoDel authors in their example implementation.
