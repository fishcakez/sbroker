sbroker
=======

`sbroker` is a library that provides the building blocks for creating a pool
and/or a load regulator. The main goals of the library are to minimise upper
percentile latency by smart queuing, easily change the feature set live with
minimal changes, easily inspect a live system and provide confidence with
property based testing.

Example
-------

Add a broker to the `sbroker` application env `brokers` and it will be started
when the application starts. Below we use a CoDel queue for the `ask` side, a
timeout queue for the `ask_r` side and no meters. Processes then call
`sbroker:ask/1` and `sbroker:ask_r` to find a match. A process calling
`sbroker:ask/1` will only match with a process that calls `sbroker:ask_r` and
vice versa.

```erlang
ok = application:load(sbroker),
Broker = broker,
Brokers = [{{local, Broker},
            {{sbroker_codel_queue, #{}}, {sbroker_timeout_queue, #{}}, []}}],
ok = application:set_env(sbroker, brokers, Brokers),
{ok, _} = application:ensure_all_started(sbroker).

Pid = spawn_link(fun() -> {go, Ref, _, _, _} = sbroker:ask_r(Broker) end),
{go, Ref, Pid, _, _} = sbroker:ask(Broker).
```

Matches can also be requested without queuing, asynchronously or using a dynamic
approach that is synchronous but becomes asynchronous if a match isn't
immediately available.

Requirements
------------

The minimum OTP version supported is 18.0.

The `sasl` application is required to start the `sbroker` application. The
`sasl` `error_logger` handler can be disabled by setting the `sasl` application
env `sasl_error_logger` to `false`.

Installing
----------

For rebar3 add `sbroker` as a depencency in `rebar.config`:

```erlang
{deps, [sbroker]}.
```

Other build tools may work if they support `rebar3` dependencies but are not
directly supported.

Testing
-------

```
$ rebar3 ct
```

Documentation
-------------

Documentation is hosted on hex: http://hexdocs.pm/sbroker/


Motivation
----------

The main roles of a pool are: dispatching, back pressure, load shedding,
worker supervision and resizing.

Existing pooling solutions assume if a worker is alive it is ready to handle
work. If a worker isn't ready a client must wait for it be ready, or error
immediately, when another worker might be ready to successfully handle the
request. If workers explicitly control when they can are available then the
pool can always dispatch to workers that are ready.

Therefore in an ideal situation clients are requesting workers and workers are
requesting clients. This is the broker pattern, where both parties are
requesting a match with the counter party. For simplicity the same API can be
used for both and so to the broker both parties are clients.

Existing pooling solutions that support back pressure use a timeout mechanism
where clients are queued for a length of time and then give up. Once clients
start timing out, the next client in the queue is likely to have waited close to
the time out. This leads to the situation where clients are all queued for
approximately the time out, either giving up or getting a worker. If clients
that give up could give up sooner then all clients would spend less time waiting
but the same number would be served.

Therefore in an ideal situation a target queue time would be chosen that keeps
the system feeling responsive and clients would give up at a rate such that in
the long term clients spend up to the target time in the queue. This is sojourn
(queue waiting) time active queue management. CoDel and PIE are two state of the
art active queue management algorithms with a target sojourn time, so should
use those with defaults that keep systems feeling responsive to a user.

Existing pooling solutions that support load shedding do not support back
pressure. These use ETS as a lock system and choose a worker to try. However
other workers might be available but are not tried or busy wait is used to retry
multiple times to gain a lock. If clients could use ETS to determine whether
a worker is likely to be available we could use existing dispatch and back
pressure mechanisms.

Therefore we want to limit access to the dispatching process by implementing a
sojourn time active queue management algorithm using ETS in front of the
dispatching process. Fortunately this is possible with the basic version of PIE.

Existing pooling solutions either don't support resize or grow the pool when no
workers are immediately available. However that worker may need to setup an
expensive resource and is unlikely to be ready immediately. If workers are
started early then the pool will be less likely to have no workers available.

However the same pools that start workers "too late" also start new workers for
every client that trys to checkout when no workers are available. However old
workers will become available again, perhaps before new workers are ready. This
often leads to too many workers getting started and wastes resources until they
are reaped for being idle. If workers are started at intervals then temporary
bursts would not start too many workers but persistent increases would still
cause adequate growth.

Therefore we want workers to be started when worker availablity is running low
but with intervals between starting workers. This can be achieved by sampling
the worker queue at intervals and starting a worker based on the reading. This
is the load regulator pattern, where the concurrency limit of tasks changes
based on sampling. For simplicity the same API as the broker could be used,
where the regulator is also the counterparty to the workers.

Existing pooling solutions that also support resizing use a temporary a
supervisor and keep restarting workers if they crash, equivalent to using max
restarts infinity. Unfortunately these pools can't recover from faults due to
bad state because the error does not bubble up the supervision tree and trigger
restarts. They are "too fault tolerant" because the error does not spread far
enough to trigger recovery. A pool where workers crash every time is not useful.

Therefore we want workers to be supervised using supervisors with any
configuration so the user can decide exactly how to handle failures. Fortunately
using both the broker and regulator patterns allows workers to be started under
user defined supervisors.

License
-------

This project is licensed under the Apache License, 2.0.

Roadmap
-------

* 1.1 - Add circuit breaker sregulator valves
* 1.2+ - Add improved queue management algorithms when possible, if at all
