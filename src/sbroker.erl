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
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%-------------------------------------------------------------------
%% @doc
%% This module provides a process match making service. A process joins one of
%% two queues and is matcheid with a process in the other queue. The queues can
%% be actively managed using an `squeue' callback module, and passively managed
%% using head or tail drop. A different strategy can be used for both queues.
%% Processes that die while in a queue are automatically removed to prevent
%% matching with a process that is nolonger alive.
%%
%% There are two functions to join a queue: `ask/1' and `ask_r/1'. Processes
%% that call `ask/1' are matched against processes that call `ask_r/1'. If no
%% match is immediately avaliable a process is queued in the relevant queue
%% until a match becomes avaliable. If queue management is used processes may be
%% dropped without a match.
%%
%% Processes calling `ask/1' try to match with/dequeue a process in the `ask_r'
%% queue. If no process exists they are queued in the `ask' queue and await a
%% process to call `ask_r/1'.
%%
%% Similarly processes calling `ask_r/1' try to match with/dequeue a process
%% in the `ask' queue. If no process exists they are queued in the `ask_r' queue
%% and await a process to call `ask/1'.
%%
%% A broker requires a callback module. The callback modules implements one
%% callback, `init/1', with single argument `Args'. `init/1' should return
%% `{ok, {AskQueueSpec, AskRQueueSpec, Interval})' or `ignore'. `AskQueuSpec' is
%% the queue specification for the `ask' queue and `AskRQueueSpec' is the queue
%% specification for the `ask_r' queue. `Interval' is the internval in `native'
%% time units that the active queue is polled. This ensures that the active
%% queue management strategy is applied even if no processes are
%% enqueued/dequeued. In the case of `ignore' the broker is not started and
%% `start_link' returns `ignore'.
%%
%% A queue specifcation takes the following form:
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
%% For example:
%%
%% ```
%% -module(sbroker_example).
%%
%% -behaviour(sbroker).
%%
%% -export([start_link/0]).
%% -export([ask/0]).
%% -export([ask_r/1]).
%% -export([init/1]).
%%
%% start_link() ->
%%     sbroker:start_link({local, ?MODULE}, ?MODULE, []).
%%
%% ask() ->
%%     sbroker:ask(?MODULE).
%%
%% ask_r() ->
%%     sbroker:ask_r(?MODULE).
%%
%% init([]) ->
%%     AskQueueSpec = {squeue_codel, {5, 100}, out, 64, drop},
%%     AskRQueueSpec = {squeue_timeout, 5000, out_r, infinity, drop},
%%     Interval = 200,
%%     {ok, {AskQueueSpec, AskRQueueSpec, Interval}}.
%% '''
-module(sbroker).

-behaviour(gen_fsm).

%% public api

-export([ask/1]).
-export([ask_r/1]).
-export([nb_ask/1]).
-export([nb_ask_r/1]).
-export([async_ask/1]).
-export([async_ask/2]).
-export([async_ask_r/1]).
-export([async_ask_r/2]).
-export([await/2]).
-export([cancel/3]).
-export([change_config/2]).
-export([len/2]).
-export([len_r/2]).
-export([start_link/2]).
-export([start_link/3]).

%% timer api

-export([timeout/1]).

%% gen_fsm api

-export([init/1]).
-export([bidding/2]).
-export([bidding/3]).
-export([asking/2]).
-export([asking/3]).
-export([handle_event/3]).
-export([handle_sync_event/4]).
-export([handle_info/3]).
-export([code_change/4]).
-export([terminate/3]).

%% types

-type broker() :: pid() | atom() | {atom(), node()} | {global, any()}
    | {via, module(), any()}.
-type name() :: {local, atom()} | {global, any()} | {via, module(), any()}.
-type start_return() :: {ok, pid()} | ignore | {error, any()}.
-type queue_spec() :: {module(), any(), out | out_r,
                       non_neg_integer() | infinity, drop | drop_r}.

-export_type([broker/0]).
-export_type([queue_spec/0]).

-callback init(Args :: any()) ->
    {ok, {queue_spec(), queue_spec(), pos_integer()}} | ignore.

-record(state, {module :: module(),
                args :: any(),
                timer :: timer:tref(),
                bidding :: sbroker_queue:drop_queue(),
                asking :: sbroker_queue:drop_queue()}).

%% public api

%% @doc Tries to match with a process calling `ask_r/1' on the same broker.
%% Returns `{go, Ref, Pid, RelativeTime, SojournTime}' on a successful match
%% or `{drop, SojournTime}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Pid' is the
%% matched process. `RelativeTime' is the time (in `native' time units) spent
%% waiting for a match after discounting time spent waiting for the broker to
%% handle requests. `SojournTime' is the time spent in both the broker's message
%% queue and internal queue, in `native' time units.
%%
%% `RelativeTime' represents the `SojournTime' without the overhead of the
%% broker. The value measures the level of queue congestion without being
%% effected by the load of the broker.
%%
%% If `RelativeTime' is positive, the request was enqueued in the internal
%% queue awaiting a match with another request sent approximately `RelativeTime'
%% after this request was sent. Therefore `SojournTime' minus `RelativeTime'
%% is the latency, or overhead, of the broker in `native' time units.
%%
%% If `RelativeTime' is negative, the request dequeued a request in the internal
%% queue that was sent approximately `RelativeTime' before this request was
%% sent. Therefore `SojournTime' is the latency, or overhead, of the broker in
%% `native' time units.
%%
%% If `RelativeTime' is `0', the request was matched with a request sent at
%% approximately the same time. Therefore `SojournTime' is the latency, or
%% overhead, of the broker in `native' time units.
%%
%% The sojourn time for `Pid' (in `native' time units) can be approximated by
%% `SojournTime' minus `RelativeTime'.
-spec ask(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask(Broker) ->
    sbroker_util:sync_send_event(Broker, ask).

%% @doc Tries to match with a process calling `ask/1' on the same broker.
%%
%% @see ask/1
-spec ask_r(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask_r(Broker) ->
    sbroker_util:sync_send_event(Broker, bid).

%% @doc Tries to match with a process calling `ask_r/1' on the same broker but
%% does not enqueue the request if no immediate match. Returns
%% `{go, Ref, Pid, RelativeTime, SojournTime}' on a successful match or
%% `{retry, SojournTime}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Pid' is the
%% matched process. `RelativeTime' is the time (in `native' time units) spent
%% waiting for a match after discounting time spent waiting for the broker to
%% handle requests. `SojournTime' is the time spent in the broker's message
%% queue in `native' time units.
%%
%% @see ask/1
-spec nb_ask(Broker) -> Go | Retry when
      Broker :: broker(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask(Broker) ->
    sbroker_util:sync_send_event(Broker, nb_ask).

%% @doc Tries to match with a process calling `ask/1' on the same broker but
%% does not enqueue the request if no immediate match.
%%
%% @see nb_ask/1
-spec nb_ask_r(Broker) -> Go | Retry when
      Broker :: broker(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask_r(Broker) ->
    sbroker_util:sync_send_event(Broker, nb_bid).

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask_r/1'. Returns `{await, Tag, Pid}'.
%%
%% `Tag' is a monitor `reference()' that uniquely identifies the reply
%% containing the result of the request. `Pid', is the pid (`pid()') of the
%% monitored broker. To cancel the request call `cancel(Pid, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Pid, RelativeTime, SojournTime}'
%% or `{Tag, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. A process making
%% multiple requests can reuse the monitor reference for subsequent requests to
%% the same broker process (`Process') using `async_ask/2'.
%%
%% @see cancel/2
%% @see async_ask/2
-spec async_ask(Broker) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: reference(),
      Pid :: pid().
async_ask(Broker) ->
    sbroker_util:async_send_event(Broker, ask).

%% @doc Sends an asynchronous request to match with a process calling `ask_r/1'.
%% Returns `{await, Tag, Pid}'.
%%
%% `Tag' is a `any()' that identifies the reply containing the result of the
%% request. `Pid', is the pid (`pid()') of the broker. To cancel all requests
%% identified by `Tag' on broker `Pid' call `cancel(Pid, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Pid, RelativeTime, SojournTime}' or
%% `{Tag, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. If the broker
%% exits or is on a disconnected node there is no guarantee of a reply and so
%% the caller should take appropriate steps to handle this scenario.
%%
%% @see cancel/2
-spec async_ask(Broker, Tag) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: any(),
      Pid :: pid().
async_ask(Broker, Tag) ->
    sbroker_util:async_send_event(Broker, ask, Tag).

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask/1'.
%%
%% @see async_ask/1
%% @see cancel/2
-spec async_ask_r(Broker) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: reference(),
      Pid :: pid().
async_ask_r(Broker) ->
    sbroker_util:async_send_event(Broker, bid).

%% @doc Sends an asynchronous request to match with a process calling `ask/1'.
%%
%% @see async_ask/2
%% @see cancel/2
-spec async_ask_r(Broker, Tag) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: any(),
      Pid :: pid().
async_ask_r(Broker, Tag) ->
    sbroker_util:async_send_event(Broker, bid, Tag).

%% @doc Await the response to an asynchronous request identified by `Tag'.
%%
%% Exits if a response is not received after `Timeout' milliseconds.
%%
%% Exits if a `DOWN' message is received with the reference `Tag'.
%%
%% @see async_ask/2
%% @see async_ask_r/2
-spec await(Tag, Timeout) -> Go | Drop when
      Tag :: any(),
      Timeout :: timeout(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
await(Tag, Timeout) ->
    receive
        {Tag, {go, _, _, _, _} = Reply} ->
            Reply;
        {Tag, {drop, _} = Reply} ->
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
%% @see async_ask_r/1
-spec cancel(Broker, Tag, Timeout) -> Count | false when
      Broker :: broker(),
      Tag :: any(),
      Timeout :: timeout(),
      Count :: pos_integer().
cancel(Broker, Tag, Timeout) ->
    gen_fsm:sync_send_event(Broker, {cancel, Tag}, Timeout).

%% @doc Change the configuration of the broker. Returns `ok' on success and
%% `{error, Reason}' on failure, where `Reason', is the reason for failure.
%%
%% Broker calls the `init/1' callback to get the new configuration. If `init/1'
%% returns `ignore' the config does not change.
-spec change_config(Broker, Timeout) -> ok | {error, Reason} when
      Broker :: broker(),
      Timeout :: timeout(),
      Reason :: any().
change_config(Broker, Timeout) ->
    gen_fsm:sync_send_all_state_event(Broker, change_config, Timeout).

%% @doc Get the length of the `ask' queue in the broker, `Broker'.
-spec len(Broker, Timeout) -> Length when
      Broker :: broker(),
      Timeout :: timeout(),
      Length :: non_neg_integer().
len(Broker, Timeout) ->
    gen_fsm:sync_send_all_state_event(Broker, ask_len, Timeout).

%% @doc Get the length of the `ask_r' queue in the broker, `Broker'.
-spec len_r(Broker, Timeout) -> Length when
      Broker :: broker(),
      Timeout :: timeout(),
      Length :: non_neg_integer().
len_r(Broker, Timeout) ->
    gen_fsm:sync_send_all_state_event(Broker, bid_len, Timeout).

%% @doc Starts a broker with callback module `Module' and argument `Args'.
-spec start_link(Module, Args) -> StartReturn when
      Module :: module(),
      Args :: any(),
      StartReturn :: start_return().
start_link(Module, Args) ->
    gen_fsm:start_link(?MODULE, {Module, Args}, []).

%% @doc Starts a broker with name `Name', callback module `Module' and argument
%% `Args'.
-spec start_link(Name, Module, Args) -> StartReturn when
      Name :: name(),
      Module :: module(),
      Args :: any(),
      StartReturn :: start_return().
start_link(Name, Module, Args) ->
    gen_fsm:start_link(Name, ?MODULE, {Module, Args}, []).

%% timer api

%% @private
-spec timeout(Broker) -> ok when
      Broker :: broker().
timeout(Broker) ->
    gen_fsm:send_event(Broker, timeout).

%% gen_fsm api

%% Inside the gen_fsm an ask_r request is refered to as a bid to make the
%% difference between ask and ask_r clearer.

%% @private
init({Module, Args}) ->
    case catch Module:init(Args) of
        {ok, {AskQueueSpec, AskRQueueSpec, Interval}} ->
            init(Module, Args, AskQueueSpec, AskRQueueSpec, Interval);
        ignore ->
            ignore;
        {'EXIT', Reason} ->
            {stop, Reason};
        Other ->
            {stop, {bad_return, {Module, init, Other}}}
    end.

%% @private
bidding({bid, Start, Bid}, State) ->
    bidding_bid(Start, Bid, State);
bidding({ask, Start, Ask}, State) ->
    bidding_ask(Start, Ask, State);
bidding(timeout, State) ->
    {next_state, bidding, bidding_timeout(State)}.

%% @private
bidding({bid, Start}, Bid, State) ->
    bidding_bid(Start, Bid, State);
bidding({nb_bid, Start}, Bid, State) ->
    retry(Start, Bid),
    {next_state, bidding, State};
bidding({ask, Start}, Ask, State) ->
    bidding_ask(Start, Ask, State);
bidding({nb_ask, Start}, Ask, State) ->
    bidding_nb_ask(Start, Ask, State);
bidding({cancel, Tag}, _From, State) ->
    {Reply, NState} = bidding_cancel(Tag, State),
    {reply, Reply, bidding, NState}.

%% @private
asking({bid, Start, Bid}, State) ->
    asking_bid(Start, Bid, State);
asking({ask, Start, Ask}, State) ->
    asking_ask(Start, Ask, State);
asking(timeout, State) ->
    {next_state, asking, asking_timeout(State)}.

%% @private
asking({bid, Start}, Bid, State) ->
    asking_bid(Start, Bid, State);
asking({nb_bid, Start}, Bid, State) ->
    asking_nb_bid(Start, Bid, State);
asking({ask, Start}, Ask, State) ->
    asking_ask(Start, Ask, State);
asking({nb_ask, Start}, Ask, State) ->
    retry(Start, Ask),
    {next_state, asking, State};
asking({cancel, Tag}, _From, State) ->
    {Reply, NState} = asking_cancel(Tag, State),
    {reply, Reply, asking, NState}.

%% @private
handle_event(Event, _, State) ->
    {stop, {bad_event, Event}, State}.

%% @private
handle_sync_event(change_config, _, StateName, State) ->
    {Reply, NState} = safe_config_change(State),
    {reply, Reply, StateName, NState};
handle_sync_event(ask_len, _, StateName, #state{asking=A} = State) ->
    Len = sbroker_queue:len(A),
    {reply, Len, StateName, State};
handle_sync_event(bid_len, _, StateName, #state{bidding=B} = State) ->
    Len = sbroker_queue:len(B),
    {reply, Len, StateName, State};
handle_sync_event(Event, _, _, State) ->
    {stop, {bad_event, Event}, State}.

%% @private
handle_info({'DOWN', MRef, _, _, _}, bidding, State) ->
    {next_state, bidding, bidding_down(MRef, State)};
handle_info({'DOWN', MRef, _, _, _}, asking, State) ->
    {next_state, asking, asking_down(MRef, State)};
handle_info(Msg, StateName, State) ->
    error_logger:error_msg("sbroker ~p received unexpected message: ~p~n",
                           [self(), Msg]),
    {next_state, StateName, State}.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, config_change(State)}.

%% @private
terminate(_Reason, _StateName, _State) ->
    ok.

%% Internal

init(Module, Args, AskQueueSpec, AskRQueueSpec, Interval) ->
    {ok, Timer} = start_intervals(Interval),
    Time = sbroker_time:native(),
    A = sbroker_queue:new(Time, AskQueueSpec),
    B = sbroker_queue:new(Time, AskRQueueSpec),
    State = #state{module=Module, args=Args, timer=Timer, asking=A, bidding=B},
    {ok, bidding, State}.

start_intervals(Interval) ->
    case sbroker_time:native_to_milli_seconds(Interval) of
        NInterval when NInterval > 0 ->
            timer:apply_interval(NInterval, sbroker, timeout, [self()]);
        MilliSeconds ->
            {error, {bad_milli_seconds, MilliSeconds}}
    end.

bidding_bid(Start, Bid, #state{bidding=B} = State) ->
    Time = sbroker_time:native(),
    NB = sbroker_queue:in(Time, Start, Bid, B),
    {next_state, bidding, State#state{bidding=NB}}.

bidding_ask(Start, Ask, #state{bidding=B, asking=A} = State) ->
    Time = sbroker_time:native(),
    case sbroker_queue:out(Time, B) of
        {{BidSojourn, {MRef, Bid}}, NB} ->
            AskSojourn = Time - Start,
            RelativeTime = max(BidSojourn - AskSojourn, 0),
            settle(MRef, RelativeTime, Bid, BidSojourn, Ask, AskSojourn),
            {next_state, bidding, State#state{bidding=NB}};
        {empty, NB} ->
            NA = sbroker_queue:in(Time, Start, Ask, A),
            {next_state, asking, State#state{bidding=NB, asking=NA}}
    end.

bidding_nb_ask(Start, Ask, #state{bidding=B} = State) ->
    Time = sbroker_time:native(),
    case sbroker_queue:out(Time, B) of
        {{BidSojourn, {MRef, Bid}}, NB} ->
            AskSojourn = Time - Start,
            RelativeTime = max(BidSojourn - AskSojourn, 0),
            settle(MRef, RelativeTime, Bid, BidSojourn, Ask, AskSojourn),
            {next_state, bidding, State#state{bidding=NB}};
        {empty, NB} ->
            retry(Time, Start, Ask),
            {next_state, asking, State#state{bidding=NB}}
    end.

asking_ask(Start, Ask, #state{asking=A} = State) ->
    Time = sbroker_time:native(),
    NA = sbroker_queue:in(Time, Start, Ask, A),
    {next_state, asking, State#state{asking=NA}}.

asking_bid(Start, Bid, #state{asking=A, bidding=B} = State) ->
    Time = sbroker_time:native(),
    case sbroker_queue:out(Time, A) of
        {{AskSojourn, {MRef, Ask}}, NA} ->
            BidSojourn = Time - Start,
            RelativeTime = min(BidSojourn - AskSojourn, 0),
            settle(MRef, RelativeTime, Bid, BidSojourn, Ask, AskSojourn),
            {next_state, asking, State#state{asking=NA}};
        {empty, NA} ->
            NB = sbroker_queue:in(Time, Start, Bid, B),
            {next_state, bidding, State#state{asking=NA, bidding=NB}}
    end.

asking_nb_bid(Start, Bid, #state{asking=A} = State) ->
    Time = sbroker_time:native(),
    case sbroker_queue:out(Time, A) of
        {{AskSojourn, {MRef, Ask}}, NA} ->
            BidSojourn = Time - Start,
            RelativeTime = min(BidSojourn - AskSojourn, 0),
            settle(MRef, RelativeTime, Bid, BidSojourn, Ask, AskSojourn),
            {next_state, asking, State#state{asking=NA}};
        {empty, NA} ->
            retry(Time, Start, Bid),
            {next_state, bidding, State#state{asking=NA}}
    end.

settle(MRef, RelativeTime, {BidPid, _} = Bid, BidSojourn, {AskPid, _} = Ask,
       AskSojourn) ->
    %% Bid always messaged first.
    gen_fsm:reply(Bid, {go, MRef, AskPid, RelativeTime, BidSojourn}),
    gen_fsm:reply(Ask, {go, MRef, BidPid, -RelativeTime, AskSojourn}),
    demonitor(MRef, [flush]).

retry(Start, From) ->
    Time = sbroker_time:native(),
    retry(Time, Start, From).

retry(Time, Start, From) ->
    gen_fsm:reply(From, {retry, Time-Start}).

bidding_cancel(Tag, #state{bidding=B} = State) ->
    Time = sbroker_time:native(),
    {Reply, NB} = sbroker_queue:cancel(Time, Tag, B),
    {Reply, State#state{bidding=NB}}.

asking_cancel(Tag, #state{asking=A} = State) ->
    Time = sbroker_time:native(),
    {Reply, NA} = sbroker_queue:cancel(Time, Tag, A),
    {Reply, State#state{asking=NA}}.

bidding_timeout(#state{bidding=B} = State) ->
    Time = sbroker_time:native(),
    NB = sbroker_queue:timeout(Time, B),
    State#state{bidding=NB}.

asking_timeout(#state{asking=A} = State) ->
    Time = sbroker_time:native(),
    NA = sbroker_queue:timeout(Time, A),
    State#state{asking=NA}.

bidding_down(MRef, #state{bidding=B} = State) ->
    Time = sbroker_time:native(),
    NB = sbroker_queue:down(Time, MRef, B),
    State#state{bidding=NB}.

asking_down(MRef, #state{asking=A} = State) ->
    Time = sbroker_time:native(),
    NA = sbroker_queue:down(Time, MRef, A),
    State#state{asking=NA}.

%% Same format of reply as sys:change_code/4,5
safe_config_change(State) ->
    try config_change(State) of
        NState ->
            {ok, NState}
    catch
        exit:Reason ->
            {{error, {'EXIT', Reason}}, State};
        error:Reason ->
            NReason = {Reason, erlang:get_stacktrace()},
            {{error, {'EXIT', NReason}}, State}
    end.

config_change(#state{module=Module, args=Args} = State) ->
    case catch Module:init(Args) of
        {ok, {AskQueueSpec, AskRQueueSpec, Interval}} ->
            config_change(AskQueueSpec, AskRQueueSpec, Interval, State);
        ignore ->
            State;
        {'EXIT', Reason} ->
            exit(Reason);
        Other ->
            exit({bad_return, {Module, init, Other}})
    end.

config_change(AskQueueSpec, AskRQueueSpec, Interval,
              #state{timer=Timer, asking=A, bidding=B} = State) ->
    NA = sbroker_queue:config_change(AskQueueSpec, A),
    NB = sbroker_queue:config_change(AskRQueueSpec, B),
    {ok, NTimer} = start_intervals(Interval),
    {ok, cancel} = timer:cancel(Timer),
    State#state{timer=NTimer, asking=NA, bidding=NB}.
