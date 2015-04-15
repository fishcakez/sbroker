sbroker
=======

Sojourn Broker - process broker for matchmaking between two groups of processes
using sojourn time based active queue management to prevent congestion.

Introduction
------------

`sbroker` provides a simple interface to match processes. One party
calls `sbroker:ask/1` and the counterparty `sbroker:ask_r/1`. If a match
is found both return `{go, Ref, Pid, SojournTime}`, where `SojournTime` is
the time spent in `native` time units waiting for a match (one will have a time
of 0), `Pid` is the other process in the match and `Ref` is the transaction
reference. If no match is found, returns `{drop, SojournTime}`.

Processes calling `sbroker:ask/1` are only matched with a process calling
`sbroker:ask_r/1` and vice versa.

Usage
-----

`sbroker` provides configurable queues defined by `sbroker:queue_spec()`s. A
`queue_spec()` takes the form:
```erlang
{Module, Args, Out, Size, Drop}
```
`Module` is an `squeue` callback module to handle active queue
management. The following modules are possible: `squeue_naive`,
`squeue_timeout`, `squeue_codel` and `squeue_codel_timeout`.
`Args` is the argument passed to the callback module. Information about
the different backends and their arguments are avaliable in the
documentation. The `native` time units are used to measure time with
these queues to prevent a lose of precision.
`sbroker_time:milli_seconds_to_native/1` converts milliseconds to the
`native` time unit.

`Out` sets the dequeue function, either the atom `out` (FIFO) or the
atom `out_r` (LIFO).

`Size` is the maximum size of the queue. Should the queue go above this
size a process is dropped. The dropping strategy is determined by
`Drop`, which is either the atom `drop` (head drop) or the atom `drop_r`
(tail drop).

An `sbroker` is started using `sbroker:start_link/2,3`:
```erlang
sbroker:start_link(Module, Args).
sbroker:start_link(Name, Module, Args).
```

The sbroker will call `Module:init(Args)`, which should return the specification
for the sbroker:
```erlang
init(_) ->
    {ok, {AskQueueSpec, AskRQueueSpec, Interval}}.
```
`AskQueueSpec` is the `queue_spec` for the queue containing processes calling
`ask/1`. The queue is referred to as the `ask` queue. Similarly
`AskRQueueSpec` is the `queue_spec` for the queue contaning processes calling
`ask_r/1`, and the queue is referedd toas the `ask_r` queue.

`Interval` is the interval in milliseconds that an `sbroker` is polled to apply
timeout queue management. Note that timeout queue management can occur on every
enqueue and dequeue, and is not reliant on the `Interval`. Setting a suitable
interval ensures that active queue management can occur if no processes are
enqueued or dequeued for a period of time.

For example:
```erlang
-module(sbroker_example).

-behaviour(sbroker).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    sbroker:start_link(?MODULE, undefined).

init(_) ->
    Timeout = sbroker_time:milli_seconds_to_native(200),
    QueueSpec = {squeue_timeout, Timeout, out, 16, drop},
    Interval = 100,
    {ok, {QueueSpec, QueueSpec, Interval}}.
```
`sbroker_example:start_link/0` will start an `sbroker` with queues configured by 
`QueueSpec`.

This configuration uses the `squeue_timeout` queue management module which drops
requests after they have been in the queue for `200` milliseconds - converted to
`native` time units. `out` sets the queue to `FIFO`. `16` sets the maximum
length of the queue. `drop` sets the queue to drop processes from the head of
the queue (head drop) when the maximum size is reached. `Interval` sets the
poll interval of the queue to `100` milliseconds, which means`squeue:timeout/2`
is called every `100` milliseconds on the active queue.

To use this `sbroker`:
```erlang
{ok, Broker} = sbroker_example:start_link(),
Pid = spawn_link(fun() -> sbroker:ask_r(Broker) end),
{go, _Ref, Pid, _, _} = sbroker:ask(Broker).
```

Build
-----
Rebar:
```
rebar compile
```
Rebar3:
```
rebar3 compile
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
Or rebar3:
```
rebar3 ct
```

License
-------
Apache License, Version 2.0
