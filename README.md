sbroker
=======

Sojourn Broker - process broker for matchmaking between two groups of processes.

Introduction
------------

Sojourn Broker is an alternative to traditional pooling approaches, it focuses
on the queues involved. Both clients and workers are explicitly enqueued into
the relevant queues and a match occurs once both a client and a worker are
queued. Both queues have many configuration options (see "Smart Queues" below)
that can reconfigured live as part of a code update or config change in a
release upgrade, or from the shell without dropping requests.

There is a simple interface to match processes. One party calls `sbroker:ask/1`
and the counterparty `sbroker:ask_r/1`. If a match is found both return
`{go, Ref, Pid, RelativeTime, SojournTime}`. `Ref` is the transaction reference.
`Pid` is the other process in the match, which can be changed to any term using
`sbroker:ask/2` and `sbroker:ask_r/2`. `SojournTime` is the time spent waiting
for a match and `RelativeTime` is difference between when the counterparty and
caller sent their requests. If no match is found, returns `{drop, SojournTime}`.

`SojournTime` and `RelativeTime` are in `native` time
units to allow conversion to the unit of choice without loss of accuracy.
However the time may not be read for every request so these values are not as
accurate as the precision suggests. The time is read using
`erlang:monotonic_time/0` and so the minimum OTP version is 18.0. For more
information on `RelativeTime` see "Relative Time and Regulation" below.

Processes calling `sbroker:ask/1` are matched with a process calling
`sbroker:ask_r/1` and vice versa. See `sbroker` for alternative functions to
request a match, such as the asynchronous `sbroker:async_ask/1`.

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
`ask/1`. The queue is referred to as the `ask` queue. Similarly `AskRQueueSpec`
is the `handler_spec` for the queue contaning processes calling `ask_r/1`, and
the queue is referred to as the `ask_r` queue.

`MeterSpec` is a `handler_spec` for a meter running on the broker. Meters are
given metric information and are called when the time is updated, see
`sbroker_meter`. The following `sbroker_meter` modules are provided:
`sbroker_overload_meter`, `sbetter_meter`, `sprotector_pie_meter`,
`sregulator_update_meter` and `sregulator_underload_meter`.

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
`QueueSpec`. This configuration uses the `sbroker_timeout_queue` callback module
which drops requests when they have been in the queue for longer than a time
limit (`200` milliseconds) and the queue length is above the minimum size (`0`).
`out` sets the queue to `FIFO`. `drop` sets the queue to drop requests from the
head or oldest request of the queue (head drop) when the maximum size (`16`) is
reached.  To use this `sbroker`:
```erlang
{ok, Broker} = sbroker_example:start_link(),
Self = self(),
Pid = spawn_link(fun() -> {go, _Ref, Self, _, _} = sbroker:ask_r(Broker) end),
{go, _Ref, Pid, _, _} = sbroker:ask(Broker).
```
For a more thorough example see "Error Isolation" below.

Build
-----
```
rebar3 compile
```

Documentation
-------------
Available at: http://hexdocs.pm/sbroker/

Otherwise:
```
rebar3 edoc
```
Then visit `doc/index.html`.

Test
----
```
rebar3 ct
```

License
-------
Apache License, Version 2.0

Error Isolation
---------------
When a worker process holds a resource, especially sockets, it can be difficult
or impossible to provide the guarantee to client processes that the resource
will always be available when the worker is called. This can be achieved by
creating the resource inside the worker's `init/1` callback and terminating as
soon as the resource is no longer available. Often this is not practical because
the supervision tree would not withstand a resource worker constantly failuring
but you want to handle the failure. For example, if a remote server went down.

Therefore the worker must handle the error. Normally this involves backing off
and retrying until the resouce is created. Usually this means that the worker
must handle requests when the resource isn't available and client must handle
the error when the resource isn't available.

If the worker does backoff it is possible that a client gets unlucky gets a
worker that doesn't have a resource when other workers are available that
have a resource. It is also possible that the client calls the worker when a
worker is busy creating the resource and has to wait for the worker to either
create the resource or fail/time out and get an error.

Sojourn broker tries to alleviate this issue by requiring the worker to choose
when it is enqueued. Therefore only available workers or resources should be in
the queue waiting for a client. This isolates the errors and allows the client
to wait for a resource to become available. If a resource becomes unavailable
while in the queue the worker can remove itself from the queue with
`sbroker:cancel/3`.

A common pattern is as follows:
```erlang

client() ->
    case sbroker:ask(sbroker_example) of
        {go, Ref, {Resource, Pid}, _, _} ->
            % stuff happens here using Resource, 3rd element set by counterparty
            Pid ! {Ref, done},
            ok;
        {drop, _} ->
            {error, drop}
    end.

init() ->
    % create any resources or backoff and retry, e.g. open gen_tcp socket
    loop(Resource).

loop(Resource) ->
    % use reverse ask call to counterparty
    case sbroker:ask_r(sbroker_example, {Resource, Pid}) of
        {go, GRef, Pid, _, _} ->
            MRef = monitor(process, Pid),
            receive
                {'DOWN', MRef, _, _, _} ->
                    % counterparty crashed, handle error before enqueuing again
                    stop(Resource);
                {GRef, done} ->
                    % counterparty finished with resource
                    demonitor(MRef, [flush]),
                    loop(Resource)
            after
                5000 ->
                    % timeout if don't hear from counterparty
                    demonitor(MRef, [flush]),
                    stop(Resource)
            end
        {drop, _} ->
            % dropped by queue
            stop(Resource)
    end.

stop(Resource) ->
    % clean up any resources
    init().
```
Usually the looping worker/resource process will be an OTP process, e.g. a
`gen_server`, and uses asynchronous queue requests using `sbroker:async_r/2,3`.

However if a resource becomes unavailable and workers are backing off then
client requests won't match and will wait in the queue until they are dropped.
In a simple timeout queue, like a `gen_server:call/3`, this means waiting for
the timeout and then failing. When an end user client is waiting in a queue it
can not distinguish between complete unavailability of the resource or a
slow/congested queue in front of the resource. Therefore queue congestion
avoidance techniques in "Smart Queues" below can be used to avoid the problem.
This may mean that the client waits for a very short period before failing,
possibly allowing for the resource to become available.

Smart Queues
------------

The primary goal of Sojourn Broker is to reduce upper tail latency. The first
step to achieving this is by only queuing a request when ready to handle a
match. Once in the queue the request has to wait for a match, which may not
occur in a reasonable time if a resource is unavailable or there are a lot of
other requests causing the queue to move slowly.

The simplest approach is to limit the length of the queue and either prevent new
requests joining the queue or drop the oldest request in the queue when the
maximum is reached. The later can prevent head of the line blocking, which can
be especially useful when newer requests overwrite the data from older requests.
Unfortunately choosing a suitable limit can be difficult because some queues are
short and slow and others are long and fast. This can mean that requests stay in
the queue for a long time, or indefinitely if the resource is not available.

The most common technique to avoid this is to add a timeout to the queue, see
`sbroker_timeout_queue`, so requests have an upper bound on the time they can
spend waiting for a match. When a timeout queue gets heavily congested the
length of the queue in time, `SojournTime`, can approach this upper bound as all
requests either timeout or get a match just before the timeout would occur
because the request immediately before it timed out.

A similar issues can occur when setting a queue size and dropping the
oldest process at the maximum. Requests are either dropped for reaching the
maximum size or get a match because the request immediately before it got
dropped. This leads to every request taking approximately the time it takes to
fill the queue.

In both cases if requests are going to be dropped it would be better to drop
them sooner, rather than later, so successful requests do not have to wait so
long. Sojourn Broker provides two modern active queue managent algorithms to
drop requests earlier that would be dropped later when using simpler strategies.
Both are configured using a `Target` sojourn time and an `Interval`, where
requests will only be dropped once the queue has been continuously above the
target for the initial interval. After this point the active queue management
will try to drop a suitable number of requests so that the queue remains near
the target.

The `Interval` value is should usually be in the 95%-99% percentile range of the
time it would take the end user client to discover the request had been dropped
and get a new request in the queue. The `Target` would then be 5%-10% of that
value for the application to feel responsive to a client.

One approach is to use the `sbroker_codel_queue`, which uses the Controlled
Delay (CoDel) active queue management algorithm. The second approach is to use
the `sprotector_pie_meter`, which uses the basic Proportional Integral
controller Enchanced (PIE) algorithm.

The CoDel queue will try to evenly but unpredictable space out drops so that
dropped clients do not retry at the same time by dropping the oldest request.
During big bursts this can lead to higher `SojournTime` values if a suitable
queue size is not set because the dropping algorithm is trying to spread out the
drops. CoDel reduces the interval between drops while the queue is above target.
This means if the counterparties are not trying to match, e.g. resources can't
be created, then the drop interval will become very small and requests will be
dropped frequently.

The PIE meter tries to only allow requests that will find a match reach the
broker, and short circuit other requests based on a self tuning probability. If
the queue is slow because no counterparties are trying to match then the drop
probability will reach 100% quickly and all requests will short circuit. This
means the queue management will also act as a circuit breaker if all requests
would be dropped/fail. The PIE meter does not combine well with a last in, first
out queue because newer requests are dequeued first and can make the queue
appear fast.

If a (misbehaving) client or application is sending significantly more requests
then it will gain a similar proportion of matches. To avoid this situation
`sbroker_fair_queue` can be used to load balance clients (or workers) on the
broker itself. The fair queue creates one queue per application, client or other
value and dequeues using a round robin strategy. Queues are created on demand
and removed when no longer used.

The CoDel algorithm works better with the fair queue than without because it can
differentiate between flows. However the PIE meter does not combine well because
it can not differentiate between the different flows and will see heavily
fluctuating `SojournTime` values.

It is also possible to load balance requests between multiple brokers with
`srand` and `sscheduler`. These choose a random broker or a broker based on
scheduler id. One of these can be combined with the `sbetter` load balancer that
tries to even out the random load balancing by occasionally comparing the
sojourn times on two random brokers and choosing the broker with the shortest
sojourn time. All three of these load balancing modules can be used to load
balance any OTP process.

Relative Time and Regulation
----------------------------
A negative `RelativeTime` means the counterparty sent their request before the
caller and the counterparty waited for the caller. A positive value means the
counterparty sent their request after the caller and the caller waited for the
counterparty. A value of `0` means the match occured "immediately" for both
requests.

Therefore `RelativeTime` is a measure of the difference in queue speeds if the
broker was so fast that it used zero processing time. This value can be used to
measure the queue congestion, e.g. more `ask` than `ask_r` requests would mean
`ask` request are waiting and get postive `RelativeTime` on matches and
`ask_r` are getting matched immediately and get negative `RelativeTime` values.

Unfortunately the broker is not infinitely fast and there is a process delaying.
The total processing time while waiting for a match can then be calculated with
`SojournTime - max(RelativeTime, 0)`.

If the broker is handling a lot of requests the `SojournTime` can grow even if
`RelativeTime` is negative, giving the false impression that the queue is
congested (though it may also be congested). The contribution of load and queue
congestion are indistinguishable and likely irrelevant to an end user client.
Therefore the `sbroker_queue` callback modules use `SojournTime` when deciding
whether to drop requests.

A group of "worker" processes whose role is to serve end user clients as quickly
as possible should try to minimise the `SojournTime` of the clients. The only
thing the worker's can do to keep the client's queue fast and uncongested is to
keep worker requests in the workers queue so clients match immediately and
aren't queued, or are queued for as little as possible. Therefore the goal of
the workers is to keep their queue slow so the client queue is fast. The
simplest method to minimise client sojourn time is to queue workers as much as
possible.

Unfortunately if workers are used then it is safe to assume the workers are
holding a resource that is expensive to create and/or maintain - otherwise they
are not required. Therefore a second goal of the workers is keep their size
minimised and so don't want to queue as many workers as possible on the broker.

A simple approach to this is to maintain a minimal group of workers and to
increase their size if a client request arrives when a worker isn't queued. In
the most naive method the client is matched with the new worker but then it has
to wait for the worker to create its resource. For example, wait for a TCP
socket to connect and possibly more handshaking, such as TLS or other
authorisation and setup. Alternatively the client waits for the next worker to
be ready, which could be the newly created worker.

However with `RelativeTime` the group of workers can continuously monitor the
queue congestion and pre-emptively create a worker before the empty worker queue
situation occurs. One approach is to try to maintain a `RelativeTime` above a
certain value by creating a worker every time it is below a certain value.
Effectively trying to keep the worker queue slightly slow so the client queue is
always fast. This is an extension of the previous approach where a worker was
created when the `RelativeTime` is less than or equal to `0`. For example,
create a worker when workers send a request less than `100` milliseconds before
clients send a request, i.e. `RelativeTime < 100` milliseconds. See `sregulator`
and `sregulator_relative_valve` for more information.

If the `SojournTime` is used to gauge queue congestion in the worker's queue,
the same issue occurs in the workers as the clients: unable to distinguish
between load and queue congestion. Therefore a load spike could be
misinterpretted as an increase in worker queue congestion, i.e. a decrease in
client queue congestion or a fast client queue. The opposite could be true if
the increase in load is due to a burst of client requests.

The `RelativeTime` can fluctate due to short bursts and the new workers may
get created but once created are no longer required. To differentiate between a
short burst and a longer term change in request rate the minimum `RelativeTime`
over an interval time can be used instead. If the local maximum is below the
target create a worker. This can be repeated with decreasing intervals until the
no longer need more workers. Decreasing consecutive intervals ensures that long
term changes are adapted too without reacting too quickly to an initial burst.
See `sregulator_codel_valve` for more information.

An `sregulator` is similar to an `sbroker`, except the regulator acts as the
`ask_r` side and controls matching with `ask` requests using an
`sregulator_valve`.

```erlang
-module(sregulator_example).

-behaviour(sregulator).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    sregulator:start_link(?MODULE, undefined, []).

init(_) ->
    QueueSpec = {sbroker_timeout_queue, {out, 200, drop, 0, 16}},
    ValveSpec = {sregulator_relative_valve, {100, 4, 16}},
    {ok, {QueueSpec, ValveSpec, []}}.
```
The `sregulator_relative_valve` will increase the size above the minimum `4`
when updated with a relative time below `100` milliseconds up to a maximum size
`16`.

Then to use the regulator:
```erlang
{go, Ref, Pid, _, _} = sregulator:ask(sregulator_example).
```

The `sregulator` is updated using the `sregulator_update_meter` in either a
`sbroker` or `sregulator`, or explicitly using `sregulator:update/3` and
`sregulator:cast/2`:
```erlang
{sregulator_update_meter, [{sregulator_example, ask_r, 100}]}
```
This will meter will update `sregulator_example` with the `RelativeTime` of the
`ask_r` queue around every 100 milliseconds.

A common pattern is as follows:
```erlang
init() ->
    {go, Ref, Pid, _, _} = sregulator:ask(sregulator_example),
    % create any resources after go from sregulator
    loop(Ref, Pid).

loop(Ref, Pid) ->
    % The regulated queue is `ask_r` so sregulator_update_meter uses `ask_r` too
    case sbroker:ask_r(sbroker_example) of
        {go, _, _, _, _} ->
            % stuff happens
            loop(Ref, Pid);
        {drop, _} ->
            % dropped by queue, maybe shrink
            drop(Ref, Pid)
    end.

drop(Ref, Pid) ->
    case sregulator:continue(Pid, Ref) of
        {go, Ref, Pid, _, _} ->
            % continue loop with same Ref as before
            loop(Ref, Pid);
        {stop, _} ->
            % process should stop its loop and Ref is removed from sregulator
            stop()
    end.

stop() ->
    % clean up any resources
    init().
```

Roadmap
-------

* 1.0 - Improve testing of edge cases (errors) in live reconfiguration
* 1.1 - Add rate limiting and circuit breaker sregulator valves
* 1.2+ - Add improved queue management algorithms when possible, if at all
