sbroker
=======

Sojourn Broker - process broker for matchmaking between two groups of processes
using sojourn time based active queue management to prevent congestion.

Introduction
------------

`sbroker` provides a simple interface to match processes. One party
calls `sbroker:ask/1` and the counterparty `sbroker:ask_r/1`. If a match
is found both return `{go, Ref, Pid, RelativeTime, SojournTime}`. `Ref` is the
transaction reference. `Pid` is the other process in the match, which can be
changed by using `sbroker:ask/2` and `sbroker:ask_r/2`. `SojournTime` is the
time spent waiting for a match and `RelativeTime` is the theoretical sojourn
time if using the sbroker had no overhead. If no match is found, returns
`{drop, SojournTime}`. The time unit for `SojournTime` and `RelativeTime` is set
by the `time_unit` start option to a broker, defaulting to the VM's native unit.

Processes calling `sbroker:ask/1` are only matched with a process calling
`sbroker:ask_r/1` and vice versa.

Usage
-----

`sbroker` provides configurable queues defined by `sbroker:handler_spec()`s. A
`handler_spec()` takes the form:
```erlang
{Module, Args}
```
`Module` is an `sbroker_queue` callback module to queue. The following callback
modules are provided: `sbroker_drop_queue`, `sbroker_timeout_queue`,
`sbroker_codel_queue` and `sbroker_fair_queue`.

`Args` is the argument passed to the callback module. Information about
the different backends and their arguments are avaliable in the
documentation.

An `sbroker` is started using `sbroker:start_link/3,4`:
```erlang
sbroker:start_link(Module, Args, Opts).
sbroker:start_link(Name, Module, Args, Opts).
```

The sbroker will call `Module:init(Args)`, which should return the specification
for the sbroker:
```erlang
init(_) ->
    {ok, {AskQueueSpec, AskRQueueSpec, [MeterSpec]}}.
```
`AskQueueSpec` is the `handler_spec` for the queue containing processes calling
`ask/1`. The queue is referred to as the `ask` queue. Similarly
`AskRQueueSpec` is the `handler_spec` for the queue contaning processes calling
`ask_r/1`, and the queue is referred to as the `ask_r` queue. `MeterSpec` is a
`handler_spec` for a meter running on the broker. Meters are given metric
information and are called when the time is updated, see `sbroker_meter`. The
following `sbroker_meter` modules are provided: `sbroker_alarm_meter`,
`sbetter_meter`, `sprotector_pie_meter` and `sregulator_meter`.

For example:
```erlang
-module(sbroker_example).

-behaviour(sbroker).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    sbroker:start_link(?MODULE, undefined, []).

init(_) ->
    QueueSpec = {sbroker_timeout_queue, {out, 200, drop, 0, 16}},
    {ok, {QueueSpec, QueueSpec, []}}.
```
`sbroker_example:start_link/0` will start an `sbroker` with queues configured by
`QueueSpec`.

This configuration uses the `sbroker_timeout_queue` callback module which drops
requests when they have been in the queue for longer than a time limit (`200`
milliseconds) and the queue length is above the minimum size (`0`). `out` sets
the queue to `FIFO`. `drop` sets the queue to drop processes from the head of
the queue (head drop) when the maximum size (`16`) is reached.

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
Testing requires rebar3:
```
rebar3 ct
```

License
-------
Apache License, Version 2.0
