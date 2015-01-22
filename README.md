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

`sbroker` provides a simple interface to match processes. One party
calls `sbroker:ask/1` and the other party `sbroker:ask_r/1`. If a match
is found both return `{go, Ref, Pid, SojournTime}`, where `SojournTime` is
the time spent in milliseconds waiting for a match (one will have a time
of 0), `Pid` is the other process in the match and `Ref` is the transaction
reference. If no match is found, returns `{drop, SojournTime}`.

Processes calling `sbroker:ask/1` are only matched with a process calling
`sbroker:ask_r/1` and vice versa.

Example
-------

```erlang
{ok, Broker} = sbroker:start_link(),
Pid = spawn_link(fun() -> sbroker:ask_r(Broker) end),
{go, _Ref, Pid, _SojournTime} = sbroker:ask(Broker).
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
sbroker:start_link(AskQueueSpec, AskRQueueSpec, Interval).
```
`AskQueueSpec` is the `queue_spec` for the queue containing processes calling
`ask/1`. The queue is referred to as the `ask` queue. Similarly
`AskRQueueSpec` is the `queue_spec` for the queue contaning processes calling
`ask_r/1`, and the queue is referedd toas the `ask_r` queue.

`Interval` is the interval in milliseconds that an `sbroker` is
polled to apply timeout queue management. Note that timeout queue
management can occur on every enqueue and dequeue, and is not reliant on
the `Interval`. Setting a suitable interval ensures that active queue
management can occur if no processes are queued or dequeued for a period
of time.

Asynchronous versions of `ask/1` and `ask_r/1` are avaliable as
`async_ask/1` and `async_ask_r/1`. On a successful match the following
message is sent:
```erlang
{AsyncRef, {go, Ref, Pid, SojournTime}}
```
Where `AsyncRef` is a monitor reference of the broker, and included in the
return values of `async_ask/1` and `async_ask_r/1`. If a match is not found:
```erlang
{AsyncRef, {drop, SojournTime}}
```

Asynchronous requests can be cancelled with `cancel/2`:

```erlang
{ok, Broker} = sbroker:start_link().
{await, AsyncRef, Broker} = sbroker:async_ask(Broker).
ok = sbroker:cancel(Broker, AsyncRef).
```
To help prevent race conditions when using asynchronous requests the
message to the `async_ask_r/1` or `ask_r/1` process is always sent before
the message to the `async_ask/1` or `ask/1` process. Therefore if the
initial message between the two groups always flows in one direction,
it may be beneficial for the receiver of that message to call
`async_ask_r/1` or `ask_r/1`, and the sender to call `async_ask/1` or `ask/1`.

A pool of worker processes using `sbroker` can be dynamically resized based
on load using `sthrottle`. `sthrottle` limits concurrency using `ask/1`,
`async_ask/1` and `done/2`. Feedback is applied manually using
`positive/1` or `negative/1`:

```erlang
%% Starts throttle with initial concurrency limit of 0.
{ok, Throttle} = sthrottle:start_link().
%% Manually positives concurrency limit by 1 (i.e. from 0 to 1):
ok = sthrottle:positive(Throttle).
%% Asks for a lock:
{go, Ref, Throttle, _SojournTime} = sthrottle:ask(Throttle).
%% Once task is complete releases lock (or exits):
ok = sthrottle:done(Throttle, Ref).
```

A feedback loop can be used when working with an `sbroker` using `signal/3`:

```erlang
{ok, Broker} = sbroker:start_link().
{ok, Throttle} = sthrottle:start_link().
ok = sthrottle:positive(Throttle).
{go, Ref, Pid, 0} = sthrottle:ask(Throttle).
%% sbroker:ask/1 request is dropped as no ask_r/1 call.
BrokerResponse = {drop, _SojournTime} = sbroker:ask(Broker).
%% Applies feedback to throttle based on broker response. In this case reduces
%% concurrency limit to 0 and the lock is lost.
{done, _SojournTime} = sthrottle:signal(Pid, Ref, BrokerResponse).
```

If the broker responds differently or the throttle is in a different
state the result of `signal/3` can be different. For all other return
values the lock is not lost, see the docs for more information.

Build
-----
Rebar builds:
```
rebar compile
```

Documentation
-------------
Available at: http://hexdocs.pm/sbroker/

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
Apache License, Version 2.0
