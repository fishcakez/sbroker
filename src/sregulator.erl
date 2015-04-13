%%-------------------------------------------------------------------
%%
%% Copyright (c) 2015, James Fish <james@fishcakez.com>
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTiES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%-------------------------------------------------------------------
%% @doc
%% This module provides a load regulation service. A process joins the queue
%% waiting to begin work. The level of concurrency is controlled by the sojourn
%% time of another queue, such as a queue for an `sbroker'. Usually this
%% means that the level of concurrency is decreased when a queue is slow (i.e.
%% workers in excess) and increased when a queue is fast (i.e. workers in
%% demand).
%%
%% When the number of active workers is below the minimum concurrency limit,
%% workers are dequeued until the minimum level is reached. When the number of
%% active workers is at (or above) the maximum, workers are never dequeued. When
%% the number of workers is greater than or equal to the minimum and less than
%% the maximum, a worker can be dequeued based on the sojourn time of a queue in
%% an `sbroker', or similar process.
%%
%% Before commencing work a worker calls `sregulator:ask/1'. If the regulator
%% returns a `go' tuple, i.e. `{go, Ref, RegulatorPid, Relative, SojournTime}',
%% the worker may continue. `Ref', the lock reference, and `RegulatorPid', the
%% pid of the regulator, are used for future communication with the regulator.
%% `RelativeTime' is `undefined' if the regulator grants the lock due to the
%% level of concurrency going below the minimum limit. Otherwise `RelativeTime'
%% is the time (in `native' time units) spent waiting for the regulator's valve
%% to dequeue the request after discounting time spent waiting for the regulator
%% to handle requests. `SojournTime' is the time spent in both the regulator's
%% message queue and internal queue, in `native' time units.
%%
%% The regulator may also return a `drop' tuple, i.e. `{drop, SojournTime}'.
%% This means that work can not begin as a lock was not acquired.
%%
%% The worker uses `update/4' to report and update the result of a successful
%% enqueuing attempt against a sbroker (or another sregulator). For example:
%% ```
%% {go, Ref, Regulator, _, _} = sregulator:ask(Regulator),
%% {go, _, _, RelativeTime, _} = sbroker:ask(Broker),
%% {continue, Ref, _, _} = sregulator:update(Regulator, Ref, RelativeTime, 500).
%% '''
%% Or `drop/2' in the case of a failure:
%% ```
%  {go, Ref, Regulator, _, _} = sregulator:ask(Regulator),
%% {drop, _} = sbroker:ask(Broker),
%% case sregulator:drop(Regulator, Ref) of
%%     {dropped, _} -> dropped;
%%     {retry, _} -> retry
%%  end.
%%  '''
%% If `drop/2' returns `{dropped, SojournTime}' the work lock is lost and the
%% process should stop working. However if it returns `{retry, SojournTime}' the
%% process should continue because dropping it would bring the number of active
%% workers below the concurrency limit. The regulator will always favour active
%% workers over queued workers, so a drop request can return a `retry' tuple
%% even when workers are queued waiting for a lock.
%%
%% The lock reference can be released using `done(Regulator, Ref)' or will be
%% automatically released when a worker exits.
%%
%% A regulator requires a callback module. The callback modules implements one
%% callback, `init/1', with single argument `Args'. `init/1' should return
%% `{ok, {QueueSpec, ValveSpec, Interval})' or `ignore'. `QueueSpec' is the
%% queue specification for the queue and `Valve' is the valve specification for
%% the queue. `Interval' is the interval in `native' time units that the queue
%% is polled. This ensures that the active queue management strategy is applied
%% even if no processes are enqueued/dequeued. In the case of `ignore' the
%% regulator is not started and `start_link' returns `ignore'.
%%
%% A queue specification takes the following form:
%% `{Module, Args, Out, Size, Drop}'. `Module' is the `squeue' callback module
%% and `Args' are its arguments. The queue is created using
%% `squeue:new(Module, Arg)'. `Out' defines the method of dequeuing, it is
%% either the atom `out' (dequeue items from the head, i.e. FIFO), or the
%% atom `out_r' (dequeue items from the tail, i.e. LIFO). `Size' is the maximum
%% size of the queue, it is either a `non_neg_integer()' or `infinity'. `Drop'
%% defines the strategy to take when the maximum size, `Size', of the queue is
%% exceeded. It is either the atom `drop' (drop from the head of the queue, i.e.
%% head drop) or `drop_r' (drop from the tail of the queue, i.e. tail drop)
%%
%% A valve specification takes the following form:
%% `{Module, Args, Min, Max}'. `Module' is the `svalve' callback module and
%% `Args' are its arguments. The valve is created using
%% `svalve:new(Module, Args)'. `Min' is the minimum desired level of
%% concurrency, a `non_neg_integer()'. `Max' is the maximum desired level of
%% concurrency and is a `non_neg_integer()' or `infinity'. The maximum must be
%% greater than or equal to the minimum.
%%
%% For example:
%%
%% ```
%% -module(sregulator_example).
%%
%% -behaviour(sregulator).
%%
%% -export([start_link/0]).
%% -export([ask/0]).
%% -export([done/1]).
%% -export([update/2]).
%% -export([init/1]).
%%
%% start_link() ->
%%     sregulator:start_link({local, ?MODULE}, ?MODULE, []).
%%
%% ask() ->
%%     sregulator:ask(?MODULE).
%%
%% done(Ref) ->
%%     sregulator:done(?MODULE, Ref).
%%
%% update(Ref, RelativeTime) ->
%%     sregulator:update(?MODULE, Ref, RelativeTime, 500).
%%
%% drop(Ref) ->
%%     sregulator:drop(?MODULE, Ref).
%%
%% init([]) ->
%%     Timeout = sbroker_time:milli_seconds_to_native(5000),
%%     Target = sbroker_time:milli_seconds_to_native(5),
%%     ValveInternal = sbroker_time:milli_second_to_native(100),
%%     QueueSpec = {squeue_timeout, Timeout, out_r, infinity, drop},
%%     ValveSpec = {svalve_codel_r, {Target, ValveInternal}, 8, 64},
%%     RegInterval = 200,
%%     {ok, {QueueSpec, ValveSpec, RegInterval}}.
%% '''
-module(sregulator).

-behaviour(gen_fsm).

%% public api

-export([ask/1]).
-export([nb_ask/1]).
-export([async_ask/1]).
-export([async_ask/2]).
-export([await/2]).
-export([cancel/3]).
-export([done/3]).
-export([update/4]).
-export([async_update/3]).
-export([async_update/4]).
-export([drop/2]).
-export([ensure_dropped/3]).
-export([change_config/2]).
-export([len/2]).
-export([size/2]).
-export([start_link/2]).
-export([start_link/3]).

%% timer api

-export([timeout/1]).

%% gen_fsm api

-export([init/1]).
-export([empty/2]).
-export([empty/3]).
-export([open/2]).
-export([open/3]).
-export([closed/2]).
-export([closed/3]).
-export([dequeue/2]).
-export([dequeue/3]).
-export([handle_event/3]).
-export([handle_sync_event/4]).
-export([handle_info/3]).
-export([code_change/4]).
-export([terminate/3]).

%% types

-type regulator() :: pid() | atom() | {atom(), node()} | {global, any()}
    | {via, module(), any()}.
-type name() :: {local, atom()} | {global, any()} | {via, module(), any()}.
-type start_return() :: {ok, pid()} | {error, any()}.
-type queue_spec() :: {module(), any(), out | out_r,
                       non_neg_integer() | infinity, drop | drop_r}.
-type valve_spec() :: {module(), any(), non_neg_integer(),
                       non_neg_integer() | infinity}.

-export_type([regulator/0]).
-export_type([queue_spec/0]).
-export_type([valve_spec/0]).

-callback init(Args :: any()) ->
    {ok, {queue_spec(), valve_spec(), pos_integer()}} | ignore.

-record(state, {module :: module(),
                args :: any(),
                min :: non_neg_integer(),
                max :: non_neg_integer() | infinity,
                timer :: timer:tref(),
                valve :: sregulator_valve:drop_valve(),
                active = gb_sets:new() :: gb_sets:set(reference())}).
%% public api

%% @doc Tries to gain a work lock with the regulator. Returns
%% `{go, Ref, Pid, RelativeTime, SojournTime}' on successfully gaining a lock or
%% `{drop, SojournTime}'.
%%
%% `Ref' is the lock reference, which is a `reference()'. `Pid' is the `pid()'
%% of the regulator. `RelativeTime' is the time (in `native' time units) spent
%% waiting for the svalve signal to dequeue the request after discounting time
%% spent waiting for the regulator to handle requests. If the regulator dequeues
%% the request due to going below the minimum concurrency limit the
%% `RelativeTime' is `undefined'. `SojournTime' is the time spent in the message
%% queue and the internal queue, in `native' time units.
%%
%% `RelativeTime' represents the `SojournTime' without some of the overhead of
%% the regulator. The value measures the level of queue congestion in the
%% without being effected by the load of the regulator.
%%
%% If `RelativeTime' is an integer, the request was enqueued in the internal
%% queue awaiting a signal request to dequeue it sent approximately
%% `RelativeTime' after this request was sent. Therefore `SojournTime' minus
%% `RelativeTime' is the latency, or overhead, of the regulator in `native' time
%% units.
-spec ask(Regulator) -> Go | Drop when
      Regulator :: regulator(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: non_neg_integer() | undefined,
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask(Regulator) ->
    sbroker_util:sync_send_event(Regulator, ask).

%% @doc Tries to gain a work lock with the regulator but does not enqueue the
%% request if a lock is not immediately available. Returns
%% `{go, Ref, Pid, undefined, SojournTime}' on a successfully gaining a lock or
%% `{retry, SojournTime}'.
%%
%% `Ref' is the lock reference, which is a `reference()'. `Pid' is the `pid()'
%% of the regulator. `undefined' reflects the fact that request is successful
%% because the regulator is below its minimum concurrency limit. `SojournTime'
%% is the time spent in the message queue of the regulator, in `native' time
%% units.
%%
%% @see ask/1
-spec nb_ask(Regulator) -> Go | Retry when
      Regulator :: regulator(),
      Go :: {go, Ref, Pid, undefined, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask(Regulator) ->
    sbroker_util:sync_send_event(Regulator, nb_ask).

%% @doc Monitors the regulator and sends an asynchronous request to gain a work
%% lock. Returns `{await, Tag, Process}'.
%%
%% `Tag' is a monitor `reference()' that uniquely identifies the reply
%% containing the result of the request. `Process', is the pid (`pid()') of the
%% monitored regulator. To cancel the request call `cancel(Process, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Pid, RelativeTime, SojournTime}'
%% or `{Tag, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% regulator and no guarantee is made of the order of replies. A process making
%% multiple requests can reuse the monitor reference for subsequent requests to
%% the same regulator process (`Process') using `async_ask/2'.
%%
%% @see cancel/2
%% @see async_ask/2
-spec async_ask(Regulator) -> {await, Tag, Pid} when
      Regulator :: regulator(),
      Tag :: reference(),
      Pid :: pid().
async_ask(Regulator) ->
    sbroker_util:async_send_event(Regulator, ask).

%% @doc Sends an asynchronous request to gain a work lock with the regulator.
%% Returns `{await, Tag, Process}'.
%%
%% `Tag' is a `any()' that identifies the reply containing the result of the
%% request. `Process', is the pid (`pid()') of the regulator. To cancel all
%% requests identified by `Tag' on regulator `Process' call
%% `cancel(Process, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Pid, RelativeTime, SojournTime}'
%% or `{Tag, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% regulator and no guarantee is made of the order of replies. If the regulator
%% exits or is on a disconnected node there is no guarantee of a reply and so
%% the caller should take appropriate steps to handle this scenario.
%%
%% @see cancel/2
-spec async_ask(Regulator, Tag) -> {await, Tag, Pid} when
      Regulator :: regulator(),
      Tag :: any(),
      Pid :: pid().
async_ask(Regulator, Tag) ->
    sbroker_util:async_send_event(Regulator, ask, Tag).

%% @doc Await the response to an asynchronous request identified by `Tag'.
%%
%% Exits if a response is not received after `Timeout' milliseconds.
%%
%% Exits if a `DOWN' message is received with the reference `Tag'.
%%
%% @see async_ask/2
%% @see async_ask_r/2
-spec await(Tag, Timeout) -> Go | Drop | Continue | NotFound when
      Tag :: any(),
      Timeout :: timeout(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: non_neg_integer() | undefined,
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime},
      Continue :: {continue, Ref, Pid, SojournTime},
      NotFound :: {not_found, SojournTime}.
await(Tag, Timeout) ->
    receive
        {Tag, {go, _, _, _, _} = Reply} ->
            Reply;
        {Tag, {drop, _} = Reply} ->
            Reply;
        {Tag, {continue, _, _, _} = Reply} ->
            Reply;
        {Tag, {not_found, _} = Reply} ->
            Reply;
        {'DOWN', Tag, _, _, Reason} when is_reference(Tag) ->
            exit({Reason, {?MODULE, await, [Tag, Timeout]}})
    after
        Timeout ->
            exit({timeout, {?MODULE, await, [Tag, Timeout]}})
    end.

%% @doc Cancels an asynchronous request. Returns the number of cancelled
%% requests or `false' if no requests exist. In the later case a caller may wish
%% to check its message queue for an existing reply.
%%
%% @see async_ask/1
%% @see async_ask/2
-spec cancel(Regulator, Tag, Timeout) -> ok | {error, not_found} when
      Regulator :: regulator(),
      Tag :: any(),
      Timeout :: timeout().
cancel(Regulator, Tag, Timeout) ->
    gen_fsm:sync_send_all_state_event(Regulator, {cancel, Tag}, Timeout).

%% @doc Release lock, `Ref', on regulator `Regulator'. Returns `ok' on success
%% and `{error, not_found}' if the lock reference does not exist on the
%% regulator.
%%
%% @see ask/1
-spec done(Regulator, Ref, Timeout) -> ok | {error, not_found} when
      Regulator :: regulator(),
      Ref :: reference(),
      Timeout :: timeout().
done(Regulator, Ref, Timeout) ->
    gen_fsm:sync_send_event(Regulator, {done, Ref}, Timeout).

%% @doc Update the regulator, `Regulator', with a relative sojourn time,
%% `RelativeTime', in `native' time units. `Ref' is the lock reference and
%% `Timeout' is the time to wait in milliseconds for a reply. Returns
%% `{continue, Ref, Pid, SojournTime}' if the lock reference, `Ref', exists on
%% the regulator. `Pid' is the pid of the regulator and `SojournTime' is the
%% time spent waiting for the regulator to handle the request. If the lock
%% reference, `Ref', does not exist returns `{not_found, SojournTime}'.
%%
%% The relative sojourn time, `RelativeTime', is used as the sojourn time in a
%% a `svalve:sojourn/4' or `svalve:sojourn_r/4' call on the valve in the
%% regulator.
-spec update(Regulator, Ref, RelativeTime, Timeout) -> Continue | NotFound when
      Regulator :: regulator(),
      Ref :: reference(),
      RelativeTime :: integer(),
      Timeout :: timeout(),
      Continue :: {continue, Ref, Pid, SojournTime},
      Pid :: pid(),
      SojournTime :: non_neg_integer(),
      NotFound :: {not_found, SojournTime}.
update(Regulator, Ref, RelativeTime, Timeout) when is_integer(RelativeTime) ->
    Sojourn = max(RelativeTime, 0),
    sbroker_util:sync_send_event(Regulator, {update, Ref, Sojourn}, Timeout).

%% @doc Asynchronously update the regulator, `Regulator', with a relative
%% sojourn time, `RelativeTime', in `native' time units. Returns
%% `{await, Tag, Pid}', where `Tag' is a monitor reference of the regulator
%% and `Pid' is the pid of the regulator. A reply is sent by the regulator
%% idenitified by the `Tag'.
%%
%% The reply if of the form `{Tag, {continue, Ref, Pid, SojournTime}}' or
%% `{Tag, {not_found, SojournTime}}'. `SojournTime' is the time between sending
%% the update request and the regulator sending a reply.
%%
%% This function is intended to allow back pressure while not blocking the
%% worker. An update request is assumed to occur at the moment a dequeue event
%% occurs. If the regulator is overloaded and has not replied to a previous
%% asynchronous update request a worker should skip updating the regulator
%% rather than delay sending the update.
-spec async_update(Regulator, Ref, RelativeTime) -> {await, Ref2, Pid} when
      Regulator :: regulator(),
      Ref :: reference(),
      RelativeTime :: integer(),
      Ref2 :: reference(),
      Pid :: pid().
async_update(Regulator, Ref, RelativeTime) when is_integer(RelativeTime) ->
    Sojourn = max(RelativeTime, 0),
    sbroker_util:async_send_event(Regulator, {update, Ref, Sojourn}).

%% @doc Asynchronous update the regulator, `Regulator', with a relative sojourn
%% time, `RelativeTime', in `native' time units. Returns `{await, Tag, Pid}',
%% where `Pid' is the pid of the regulator.
%%
%% The reply if of the form `{Tag, {continue, Ref, Pid, SojournTime}}' or
%% `{Tag, {not_found, SojournTime}}'. `SojournTime' is the time between sending
%% the update request and the regulator sending a reply.
%%
%% This function behaves the same as `async_update/3' except the regulator is
%% not monitored and the supplied tag, `Tag', is used in the reply. If the
%% regulator exits there is no guarantee of a reply.
-spec async_update(Regulator, Ref, RelativeTime, Tag) -> {await, Tag, Pid} when
      Regulator :: regulator(),
      Ref :: reference(),
      RelativeTime :: integer(),
      Tag :: any(),
      Pid :: pid().
async_update(Regulator, Ref, RelativeTime, Tag) when is_integer(RelativeTime) ->
    Sojourn = max(RelativeTime, 0),
    sbroker_util:async_send_event(Regulator, {update, Ref, Sojourn}, Tag).

%% @doc Signal a drop. Returns `ok' if the lock is released, `{error, retry}' if
%% the lock is maintained and `{error, not_found}' if the lock does not exist on
%% the regulator.
%%
%% This function can be used to signal to the regulator that an event has
%% occured that should shrink the level of concurrency. For example a connection
%% process that fails to connect to a remote server may call `drop/2' so that
%% the concurrency level decreases when the remote server is unavailable.
%%
%% @see ask/1
-spec drop(Regulator, Ref) -> Response when
      Regulator :: regulator(),
      Ref :: reference(),
      Response :: {dropped | retry | not_found, SojournTime},
      SojournTime :: non_neg_integer().
drop(Regulator, Ref) ->
    sbroker_util:sync_send_event(Regulator, {drop, Ref}).

%% @doc Signal a drop and release the lock. Returns `ok' if the lock is
%% released and `{error, not_found}' if the lock does not exist on the
%% regulator.
%%
%% Unlike `drop/2' the lock is always released if it exists.
%%
%% @see ask/1
%% @see drop/2
-spec ensure_dropped(Regulator, Ref, Timeout) -> ok | {error, not_found} when
      Regulator :: regulator(),
      Ref :: reference(),
      Timeout :: timeout().
ensure_dropped(Regulator, Ref, Timeout) ->
    sbroker_util:sync_send_event(Regulator, {ensure_dropped, Ref}, Timeout).

%% @doc Change the configuration of the regulator. Returns `ok' on success and
%% `{error, Reason}' on failure, where `Reason', is the reason for failure.
%%
%% Regulator calls the `init/1' callback to get the new configuration. If
%% `init/1' returns `ignore' the config does not change.
%%
%% @see start_link/2
-spec change_config(Regulator, Timeout) -> ok | {error, Reason} when
      Regulator :: regulator(),
      Timeout :: timeout(),
      Reason :: any().
change_config(Regulator, Timeout) ->
    gen_fsm:sync_send_all_state_event(Regulator, change_config, Timeout).

%% @doc Get the length of the queue in the regulator, `Regulator'.
-spec len(Regulator, Timeout) -> Length when
      Regulator :: regulator(),
      Timeout :: timeout(),
      Length :: non_neg_integer().
len(Regulator, Timeout) ->
    gen_fsm:sync_send_all_state_event(Regulator, len, Timeout).

%% @doc Get the number of active process using the regulator, `Regulator'.
-spec size(Regulator, Timeout) -> Size when
      Regulator :: regulator(),
      Timeout :: timeout(),
      Size :: non_neg_integer().
size(Regulator, Timeout) ->
    gen_fsm:sync_send_all_state_event(Regulator, size, Timeout).

%% @doc Starts a regulator with callback module `Module' and argument `Args'.
-spec start_link(Module, Args) -> StartReturn when
      Module :: module(),
      Args :: any(),
      StartReturn :: start_return().
start_link(Module, Args) ->
    gen_fsm:start_link(?MODULE, {Module, Args}, [{debug, []}]).

%% @doc Starts a regulator with name `Name', callback module `Module' and
%% argument `Args'.
%%
%% @see start_link/2
-spec start_link(Name, Module, Args) -> StartReturn when
      Name :: name(),
      Module :: module(),
      Args :: any(),
      StartReturn :: start_return().
start_link(Name, Module, Args) ->
    gen_fsm:start_link(Name, ?MODULE, {Module, Args}, []).

%% timer api

%% @private
-spec timeout(Regulator) -> ok when
      Regulator :: regulator().
timeout(Regulator) ->
    gen_fsm:send_event(Regulator, timeout).

%% gen_fsm api

%% @private
init({Module, Args}) ->
    case catch Module:init(Args) of
        {ok, {QueueSpec, ValveSpec, Interval}} ->
            init(Module, Args, QueueSpec, ValveSpec, Interval);
        ignore ->
            ignore;
        {'EXIT', Reason} ->
            {stop, Reason};
        Other ->
            {stop, {bad_return, {Module, init, Other}}}
    end.

%% @private
empty({{update, Ref, Sojourn}, Start, From}, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            empty_update(Start, Ref, Sojourn, From, State);
        false ->
            not_found(Start, From, empty, State)
    end;
empty({ask, Start, From}, State) ->
    empty_ask(Start, From, State);
empty(timeout, State) ->
    {next_state, empty, handle_timeout(State)}.

%% @private
empty({{update, Ref, Sojourn}, Start}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            empty_update(Start, Ref, Sojourn, From, State);
        false ->
            not_found(Start, From, empty, State)
    end;
empty({{drop, Ref}, Start}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            empty_drop(Start, From, State);
        false ->
            not_found(Start, From, empty, State)
    end;
empty({{ensure_dropped, Ref}, Start}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            empty_ensure_dropped(Ref, Start, From, State);
        false ->
            {reply, {error, not_found}, empty, State}
    end;
empty({ask, Start}, From, State) ->
    empty_ask(Start, From, State);
empty({nb_ask, Start}, From, State) ->
    empty_ask(Start, From, State);
empty({done, Ref}, _, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            empty_done(Ref, State);
        false ->
            {reply, {error, not_found}, empty, State}
    end.

%% @private
open({{update, Ref, Sojourn}, Start, From}, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            open_update(Start, Ref, Sojourn, From, State);
        false ->
            not_found(Start, From, open, State)
    end;
open({ask, Start, From}, State) ->
    open_ask(Start, From, State);
open(timeout, State) ->
    {next_state, open, handle_timeout(State)}.

%% @private
open({{update, Ref, Sojourn}, Start}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            open_update(Start, Ref, Sojourn, From, State);
        false ->
            not_found(Start, From, open, State)
    end;
open({{drop, Ref}, Start}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            open_drop(Ref, Start, From, State);
        false ->
            not_found(Start, From, open, State)
    end;
open({{ensure_dropped, Ref}, Start}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            open_ensure_dropped(Ref, Start, From, State);
        false ->
            {reply, {error, not_found}, open, State}
    end;
open({ask, Start}, From, State) ->
    open_ask(Start, From, State);
open({nb_ask, Start}, From, State) ->
    Time = sbroker_time:native(),
    retry(From, Time-Start),
    {next_state, open, State};
open({done, Ref}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            open_done(Ref, From, State);
        false ->
            {reply, {error, not_found}, open, State}
    end.

%% @private
closed({{update, Ref, Sojourn}, Start, From}, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            closed_update(Start, Ref, Sojourn, From, State);
        false ->
            not_found(Start, From, closed, State)
    end;
closed({ask, Start, From}, State) ->
    closed_ask(Start, From, State);
closed(timeout, State) ->
    {next_state, closed, handle_timeout(State)}.

%% @private
closed({{update, Ref, Sojourn}, Start}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            closed_update(Start, Ref, Sojourn, From, State);
        false ->
            not_found(Start, From, closed, State)
    end;
closed({{drop, Ref}, Start}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            closed_drop(Ref, Start, From, State);
        false ->
            not_found(Start, From, closed, State)
    end;
closed({{ensure_dropped, Ref}, Start}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            closed_ensure_dropped(Ref, Start, From, State);
        false ->
            {reply, {error, not_found}, closed, State}
    end;
closed({ask, Start}, From, State) ->
    closed_ask(Start, From, State);
closed({nb_ask, Start}, From, State) ->
    Time = sbroker_time:native(),
    retry(From, Time-Start),
    {next_state, closed, State};
closed({done, Ref}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            closed_done(Ref, From, State);
        false ->
            {reply, {error, not_found}, closed, State}
    end.

%% @private
dequeue(Event, State) ->
    case dequeue(State) of
        {next_state, empty, NState} ->
            empty(Event, NState);
        {next_state, open, NState} ->
            open(Event, NState);
        {next_state, closed, NState} ->
            closed(Event, NState)
    end.

%% @private
dequeue({done, Ref}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true ->
            dequeue_done(Ref, From, State);
        false ->
            gen_fsm:reply(From, {error, not_found}),
            dequeue(State)
    end;
dequeue(Event, From, State) ->
    case dequeue(State) of
        {next_state, empty, NState} ->
            empty(Event, From, NState);
        {next_state, open, NState} ->
            open(Event, From, NState);
        {next_state, closed, NState} ->
            closed(Event, From, NState)
    end.

%% @private
handle_event(Event, _, State) ->
    {stop, {bad_event, Event}, State}.

%% @private
handle_sync_event(change_config, _, StateName, State) ->
    {Reply, NStateName, NState} = safe_config_change(StateName, State),
    {reply, Reply, NStateName, NState};
handle_sync_event({cancel, Tag}, From, dequeue, State) ->
    {Reply, NState} = handle_cancel(Tag, State),
    gen_fsm:reply(From, Reply),
    dequeue(NState);
handle_sync_event({cancel, Tag}, _, StateName, State) ->
    {Reply, NState} = handle_cancel(Tag, State),
    {reply, Reply, StateName, NState};
handle_sync_event(len, _, StateName, #state{valve=V} = State) ->
    Len = sregulator_valve:len(V),
    {reply, Len, StateName, State};
handle_sync_event(size, _, StateName, #state{active=Active} = State) ->
    Size = gb_sets:size(Active),
    {reply, Size, StateName, State}.

%% @private
handle_info({'DOWN', Ref, _, _, _}, StateName, #state{active=Active} = State) ->
    case gb_sets:is_element(Ref, Active) of
        true when StateName =:= empty ->
            empty_down(Ref, State);
        true when StateName =:= open ->
            open_down(Ref, State);
        true when StateName =:= closed ->
            closed_down(Ref, State);
        true when StateName =:= dequeue ->
            dequeue_down(Ref, State);
        false when StateName =:= dequeue ->
            NState = handle_ask_down(Ref, State),
            dequeue(NState);
        false ->
            {next_state, StateName, handle_ask_down(Ref, State)}
    end.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    config_change(StateName, State).

%% @private
terminate(_Reason, _StateName, _State) ->
    ok.

%% Internal

init(Module, Args, QueueSpec, {VModule, VArgs, Min, Max}, Interval)
  when is_integer(Min) andalso Min >= 0 andalso
       ((is_integer(Max) andalso Max >= Min) orelse Max =:= infinity) ->
    {ok, Timer} = start_intervals(Interval),
    Time = sbroker_time:native(),
    V = sregulator_valve:new(Time, {VModule, VArgs, QueueSpec}),
    NV = sregulator_valve:close(V),
    State = #state{module=Module, args=Args, min=Min, max=Max,
                   timer=Timer, valve=NV},
    case Min of
        0 when Max =:= 0 ->
            {ok, closed, State};
        0 ->
            {ok, open, State#state{valve=sregulator_valve:open(NV)}};
        _ ->
            {ok, empty, State}
    end.

start_intervals(Interval) ->
    case sbroker_time:native_to_milli_seconds(Interval) of
        NInterval when NInterval > 0 ->
            timer:apply_interval(NInterval, sregulator, timeout, [self()]);
        MilliSeconds ->
            {error, {bad_milli_seconds, MilliSeconds}}
    end.

empty_update(OutTime, Ref, Sojourn, From, State) ->
    NState = handle_closed_update(OutTime, Ref, Sojourn, From, State),
    {next_state, empty, NState}.

open_update(OutTime, Ref, Sojourn, From,
            #state{valve=V, active=Active} = State) ->
    Time = sbroker_time:native(),
    SojournTime = Time - OutTime,
    continue(From, Ref, SojournTime),
    case sregulator_valve:sojourn(Time, OutTime, Sojourn, V) of
        {{SojournTime2, {Ref2, From2}}, NV} ->
            RelativeTime = max(SojournTime2 - SojournTime, 0),
            go(From2, Ref2, RelativeTime, SojournTime2),
            NActive = gb_sets:insert(Ref2, Active),
            open_increased(State#state{valve=NV, active=NActive});
       {_, NV} ->
            {next_state, open, State#state{valve=NV}}
    end.

closed_update(OutTime, Ref, Sojourn, From, State) ->
    NState = handle_closed_update(OutTime, Ref, Sojourn, From, State),
    {next_state, closed, NState}.

handle_closed_update(OutTime, Ref, Sojourn, From, #state{valve=V} = State) ->
    Time = sbroker_time:native(),
    continue(From, Ref, Time - OutTime),
    {closed, NV} = sregulator_valve:sojourn(Time, OutTime, Sojourn, V),
    State#state{valve=NV}.

open_increased(#state{max=Max, valve=V, active=Active} = State) ->
    case gb_sets:size(Active) of
        Max ->
            {next_state, closed, State#state{valve=sregulator_valve:close(V)}};
        _ ->
            {next_state, open, State}
    end.

empty_ask(Start, {Pid, _} = From,
          #state{min=Min, max=Max, valve=V, active=Active} = State) ->
    Ref = monitor(process, Pid),
    Time = sbroker_time:native(),
    go(From, Ref, undefined, Time-Start),
    NActive = gb_sets:insert(Ref, Active),
    NState = State#state{active=NActive},
    case gb_sets:size(NActive) of
        Max ->
            {next_state, closed, NState};
        Min ->
            {next_state, open, NState#state{valve=sregulator_valve:open(V)}};
        _ ->
            {next_state, empty, NState}
    end.

open_ask(Start, From, State) ->
    {next_state, open, enqueue_ask(Start, From, State)}.

closed_ask(Start, From, State) ->
    {next_state, closed, enqueue_ask(Start, From, State)}.

enqueue_ask(Start, From, #state{valve=V} = State) ->
    Time = sbroker_time:native(),
    State#state{valve=sregulator_valve:in(Time, Start, From, V)}.

empty_drop(DropTime, From, State) ->
    {next_state, empty, closed_drop(DropTime, From, State)}.

open_drop(Ref, DropTime, From, #state{active=Active, min=Min} = State) ->
    case gb_sets:size(Active) of
        Min ->
            open_min_drop(DropTime, From, State);
        _ ->
            do_open_drop(Ref, DropTime, From, State)
    end.

open_min_drop(DropTime, From, #state{valve=V, active=Active} = State) ->
    Time = sbroker_time:native(),
    SojournTime = Time - DropTime,
    retry(From, SojournTime),
    case sregulator_valve:dropped(Time, DropTime, V) of
        {{SojournTime2, {Ref2, From2}}, NV} ->
            RelativeTime = max(SojournTime2 - SojournTime, 0),
            go(From2, Ref2, RelativeTime, SojournTime2),
            NActive = gb_sets:insert(Ref2, Active),
            open_increased(State#state{valve=NV, active=NActive});
        {_, NV} ->
            {next_state, open, State#state{valve=NV}}
    end.

do_open_drop(Ref, DropTime, From, State) ->
    case handle_drop(Ref, DropTime, From, State) of
        {steady, NState} ->
            {next_state, open, NState};
        {decreased, NState} ->
            open_decreased(NState)
    end.

closed_drop(Ref, DropTime, From,
            #state{min=Min, max=Max, active=Active} = State) ->
    case gb_sets:size(Active) of
        Min ->
            {next_state, closed, closed_drop(DropTime, From, State)};
        Max ->
            closed_max_drop(Ref, DropTime, From, State);
        _ ->
            {_, NState} = handle_drop(Ref, DropTime, From, State),
            {next_state, closed, NState}
    end.

closed_drop(DropTime, From, #state{valve=V} = State) ->
    Time = sbroker_time:native(),
    retry(From, Time-DropTime),
    {closed, NV} = sregulator_valve:dropped(Time, DropTime, V),
    State#state{valve=NV}.

closed_max_drop(Ref, DropTime, From, #state{valve=V} = State) ->
    NState = State#state{valve=sregulator_valve:open(V)},
    case handle_drop(Ref, DropTime, From, NState) of
        {steady, #state{valve=NV} = NState2} ->
            NState3 = NState2#state{valve=sregulator_valve:close(NV)},
            {next_state, closed, NState3};
        {decreased, NState2} ->
            {next_state, open, NState2}
    end.

handle_drop(Ref, DropTime, From, #state{valve=V, active=Active} = State) ->
    demonitor(Ref, [flush]),
    Time = sbroker_time:native(),
    SojournTime = Time - DropTime,
    dropped(From, SojournTime),
    case sregulator_valve:dropped(Time, DropTime, V) of
        {{SojournTime2, {Ref2, From2}}, NV} ->
            RelativeTime = max(SojournTime2 - SojournTime, 0),
            go(From2, Ref2, RelativeTime, SojournTime2),
            NActive = gb_sets:insert(Ref2, gb_sets:delete(Ref, Active)),
            {steady, State#state{valve=NV, active=NActive}};
        {_, NV} ->
            NActive = gb_sets:delete(Ref, Active),
            {decreased, State#state{valve=NV, active=NActive}}
    end.

open_decreased(#state{min=Min, valve=V, active=Active} = State) ->
    case gb_sets:size(Active) of
        Size when Size < Min ->
            NState = State#state{valve=sregulator_valve:close(V)},
            {next_state, empty, NState};
        _ ->
            {next_state, open, State}
    end.

closed_decreased(#state{min=Min, max=Max, valve=V, active=Active} = State) ->
    case gb_sets:size(Active) of
        Size when Size < Min ->
            {next_state, empty, State};
        Size when Size < Max ->
            {next_state, open, State#state{valve=sregulator_valve:open(V)}};
        _ ->
            {next_state, closed, State}
    end.

not_found(Start, From, StateName, State) ->
    Time = sbroker_time:native(),
    not_found(From, Time-Start),
    {next_state, StateName, State}.

empty_ensure_dropped(Ref, DropTime, From, State) ->
    {_, NState} = handle_ensure_dropped(Ref, DropTime, From, State),
    {next_state, empty, NState}.

handle_ensure_dropped(Ref, DropTime, From,
                      #state{valve=V, active=Active} = State) ->
    demonitor(Ref, [flush]),
    gen_fsm:reply(From, ok),
    Time = sbroker_time:native(),
    case sregulator_valve:dropped(Time, DropTime, V) of
        {{SojournTime2, {Ref2, From2}}, NV} ->
            SojournTime = Time - DropTime,
            RelativeTime = max(SojournTime2 - SojournTime, 0),
            go(From2, Ref2, RelativeTime, SojournTime2),
            NActive = gb_sets:insert(Ref2, gb_sets:delete(Ref, Active)),
            {steady, State#state{valve=NV, active=NActive}};
        {_, NV} ->
            NActive = gb_sets:delete(Ref, Active),
            {decreased, State#state{valve=NV, active=NActive}}
    end.

open_ensure_dropped(Ref, DropTime, From,
                    #state{active=Active, min=Min} = State) ->
    case gb_sets:size(Active) of
        Min ->
            open_min_dropped(Ref, DropTime, From, State);
        _ ->
            {_, NState} = handle_ensure_dropped(Ref, DropTime, From, State),
            {next_state, open, NState}
    end.

open_min_dropped(Ref, DropTime, From, State) ->
    case handle_replace(Ref, From, State) of
        {steady, Time, NState} ->
            open_min_dropped(Time, DropTime, NState);
        {empty, Time, #state{valve=NV} = NState} ->
            NV2 = sregulator_valve:close(NV),
            {closed, NV3} = sregulator_valve:dropped(Time, DropTime, NV2),
            {next_state, empty, NState#state{valve=NV3}}
    end.

open_min_dropped(Time, DropTime, #state{valve=V, active=Active} = State) ->
    case sregulator_valve:dropped(Time, DropTime, V) of
        {{SojournTime, {Ref, From}}, NV} ->
            RelativeTime = max(SojournTime - (Time-DropTime), 0),
            go(From, Ref, RelativeTime, SojournTime),
            NActive = gb_sets:insert(Ref, Active),
            open_increased(State#state{valve=NV, active=NActive});
        {_, NV} ->
            {next_state, open, State#state{valve=NV}}
    end.

handle_replace(Ref, From, #state{valve=V, active=Active} = State) ->
    demonitor(Ref, [flush]),
    gen_fsm:reply(From, ok),
    Time = sbroker_time:native(),
    case sregulator_valve:out(Time, V) of
        {{SojournTime2, {Ref2, From2}}, NV} ->
            go(From2, Ref2, undefined, SojournTime2),
            NActive = gb_sets:insert(Ref2, gb_sets:delete(Ref, Active)),
            {steady, Time, State#state{valve=NV, active=NActive}};
        {empty, NV} ->
            NActive = gb_sets:delete(Ref, Active),
            {empty, Time, State#state{valve=NV, active=NActive}}
    end.

closed_ensure_dropped(Ref, DropTime, From,
                      #state{min=Min, max=Max, active=Active} = State) ->
    case gb_sets:size(Active) of
        Min ->
            closed_min_dropped(Ref, DropTime, From, State);
        Max ->
            closed_max_dropped(Ref, DropTime, From, State);
        _ ->
            {_, NState} = handle_ensure_dropped(Ref, DropTime, From, State),
            {next_state, closed, NState}
    end.

closed_min_dropped(Ref, DropTime, From, State) ->
    {Status, Time, NState} = handle_replace(Ref, From, State),
    {closed, NV2} = sregulator_valve:dropped(Time, DropTime,
                                             NState#state.valve),
    NState2 = NState#state{valve=NV2},
    case Status of
        steady ->
            {next_state, closed, NState2};
        empty ->
            {next_state, empty, NState2}
    end.

closed_max_dropped(Ref, DropTime, From, #state{valve=V} = State) ->
    NState = State#state{valve=sregulator_valve:open(V)},
    case handle_ensure_dropped(Ref, DropTime, From, NState) of
        {steady, #state{valve=NV} = NState2} ->
            NState3 = NState2#state{valve=sregulator_valve:close(NV)},
            {next_state, closed, NState3};
        {decreased, NState2} ->
            {next_state, open, NState2}
    end.

empty_done(Ref, #state{active=Active} = State) ->
    NActive = gb_sets:delete(Ref, Active),
    demonitor(Ref, [flush]),
    {reply, ok, empty, State#state{active=NActive}}.

open_done(Ref, From, State) ->
    case handle_done(Ref, From, State) of
        {steady, NState} ->
            {next_state, open, NState};
        {decreased, NState} ->
            open_decreased(NState)
    end.

closed_done(Ref, From, State) ->
    case handle_done(Ref, From, State) of
        {steady, NState} ->
            {next_state, closed, NState};
        {decreased, NState} ->
            closed_decreased(NState)
    end.

dequeue_done(Ref, From, #state{active=Active} = State) ->
    demonitor(Ref, [flush]),
    gen_fsm:reply(From, ok),
    NActive = gb_sets:delete(Ref, Active),
    dequeue(State#state{active=NActive}).

handle_done(Ref, From, State) ->
    demonitor(Ref, [flush]),
    gen_fsm:reply(From, ok),
    handle_down(Ref, State).

empty_down(Ref, #state{active=Active} = State) ->
    NActive = gb_sets:delete(Ref, Active),
    demonitor(Ref, [flush]),
    {next_state, empty, State#state{active=NActive}}.

open_down(Ref, State) ->
    case handle_down(Ref, State) of
        {steady, NState} ->
            {next_state, open, NState};
        {decreased, NState} ->
            open_decreased(NState)
    end.

closed_down(Ref, State) ->
    case handle_down(Ref, State) of
        {steady, NState} ->
            {next_state, closed, NState};
        {decreased, NState} ->
            closed_decreased(NState)
    end.

handle_down(Ref, #state{valve=V, active=Active} = State) ->
    NActive = gb_sets:delete(Ref, Active),
    Time = sbroker_time:native(),
    case sregulator_valve:out(Time, V) of
        {{SojournTime2, {Ref2, From2}}, NV} ->
            go(From2, Ref2, undefined, SojournTime2),
            NActive2 = gb_sets:insert(Ref2, NActive),
            {steady, State#state{valve=NV, active=NActive2}};
        {empty, NV} ->
            {decreased, State#state{valve=NV, active=NActive}}
    end.

go(From, Ref, RelativeTime, SojournTime) ->
    gen_fsm:reply(From, {go, Ref, self(), RelativeTime, SojournTime}).

retry(From, SojournTime) ->
    gen_fsm:reply(From, {retry, SojournTime}).

dropped(From, SojournTime) ->
    gen_fsm:reply(From, {dropped, SojournTime}).

not_found(From, SojournTime) ->
    gen_fsm:reply(From, {not_found, SojournTime}).

continue(From, Ref, SojournTime) ->
    gen_fsm:reply(From, {continue, Ref, self(), SojournTime}).

handle_timeout(#state{valve=V} = State) ->
    Time = sbroker_time:native(),
    NV = sregulator_valve:timeout(Time, V),
    State#state{valve=NV}.

handle_cancel(Tag, #state{valve=V} = State) ->
    Time = sbroker_time:native(),
    {Reply, NV} = sregulator_valve:cancel(Time, Tag, V),
    {Reply, State#state{valve=NV}}.

handle_ask_down(Ref, #state{valve=V} = State) ->
    Time = sbroker_time:native(),
    State#state{valve=sregulator_valve:down(Time, Ref, V)}.

safe_config_change(StateName, State) ->
    try
        config_change(StateName, State)
    catch
        exit:Reason ->
            {{error, {'EXIT', Reason}}, StateName, State};
        error:Reason ->
            NReason = {Reason, erlang:get_stacktrace()},
            {{error, {'EXIT', NReason}}, StateName, State}
    end.

config_change(StateName, #state{module=Module, args=Args} = State) ->
    case catch Module:init(Args) of
        {ok, {QueueSpec, ValveSpec, Interval}} ->
            config_change(QueueSpec, ValveSpec, Interval, State);
        ignore ->
            {ok, StateName, State};
        {'EXIT', Reason} ->
            exit(Reason);
        Other ->
            exit({bad_return, {Module, init, Other}})
    end.

config_change(QueueSpec, {VModule, VArgs, Min, Max}, Interval,
              #state{timer=Timer, valve=V, active=Active} = State)
  when is_integer(Min) andalso Min >= 0 andalso
       ((is_integer(Max) andalso Max >= Min) orelse Max =:= infinity) ->
    NV = sregulator_valve:config_change({VModule, VArgs, QueueSpec}, V),
    {ok, NTimer} = start_intervals(Interval),
    {ok, cancel} = timer:cancel(Timer),
    NState = State#state{timer=NTimer, min=Min, max=Max},
    case gb_sets:size(Active) of
        Size when Size < Min ->
            {ok, dequeue, NState#state{valve=sregulator_valve:close(NV)}};
        Size when Size >= Max ->
            {ok, closed, NState#state{valve=sregulator_valve:close(NV)}};
        _ ->
            {ok, open, NState#state{valve=sregulator_valve:open(NV)}}
    end.

dequeue(#state{min=Min, valve=V, active=Active, max=Max} = State) ->
    Diff = Min - gb_sets:size(Active),
    Time = sbroker_time:native(),
    {NV, NActive} = dequeue_loop(sregulator_valve:out(Time, V), Active, Diff),
    case gb_sets:size(NActive) of
        Max ->
            NV2 = sregulator_valve:close(NV),
            NState = State#state{valve=NV2, active=NActive},
            {next_state, closed, NState};
        Min ->
            NV2 = sregulator_valve:open(NV),
            NState = State#state{valve=NV2, active=NActive},
            {next_state, open, NState};
        _ ->
            NV2 = sregulator_valve:close(NV),
            NState = State#state{valve=NV2, active=NActive},
            {next_state, empty, NState}
    end.

dequeue_loop({empty, V}, Active, _) ->
    {V, Active};
dequeue_loop({{SojournTime, {Ref, From}}, V}, Active, 1) ->
    go(From, Ref, undefined, SojournTime),
    {V, gb_sets:insert(Ref, Active)};
dequeue_loop({{SojournTime, {Ref, From}}, V}, Active, N) ->
    go(From, Ref, undefined, SojournTime),
    dequeue_loop(sregulator_valve:out(V), gb_sets:insert(Ref, Active), N-1).

dequeue_down(Ref, #state{active=Active} = State) ->
    NActive = gb_sets:delete(Ref, Active),
    dequeue(State#state{active=NActive}).
