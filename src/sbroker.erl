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
%% two queues and is matches with a process in the other queue. The queues can
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
-export([cancel/2]).
-export([change_config/2]).

-export([start_link/2]).
-export([start_link/3]).


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

%% test api

-export([force_timeout/1]).

%% types

-type broker() :: pid() | atom() | {atom(), node()} | {global, any()}
    | {via, module(), any()}.
-type name() :: {local, atom()} | {global, any()} | {via, module(), any()}.
-type start_return() :: {ok, pid()} | {error, any()}.
-type queue_spec() :: {module(), any(), out | out_r,
                       non_neg_integer() | infinity, drop | drop_r}.

-export_type([broker/0]).
-export_type([queue_spec/0]).

-callback init(Args :: any()) ->
    {ok, {queue_spec(), queue_spec(), pos_integer()}} | ignore.

-record(state, {module :: module(),
                args :: any(),
                timer :: sbroker_timer:timer(),
                bidding :: sbroker_queue:drop_queue(),
                asking :: sbroker_queue:drop_queue()}).

%% public api

%% @doc Tries to match with a process calling `ask_r/1' on the same broker.
%% Returns `{go, Ref, Pid, SojournTime}' on a successful match or
%% `{drop, SojournTime}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Pid' is the
%% matched process. `SojournTime' is the time spent in the queue in
%% milliseconds.
-spec ask(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Pid, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask(Broker) ->
    gen_fsm:sync_send_event(Broker, ask, infinity).

%% @doc Tries to match with a process calling `ask/1' on the same broker.
%%
%% @see ask/1
-spec ask_r(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Pid, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask_r(Broker) ->
    gen_fsm:sync_send_event(Broker, bid, infinity).

%% @doc Tries to match with a process calling `ask_r/1' on the same broker but
%% does not enqueue the request if no immediate match. Returns
%% `{go, Ref, Pid, 0}' on a successful match or `{retry, 0}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Pid' is the
%% matched process. `0' reflects the fact that no time was spent in the queue.
%%
%% @see ask/1
-spec nb_ask(Broker) -> Go | Retry when
      Broker :: broker(),
      Go :: {go, Ref, Pid, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      SojournTime :: 0,
      Retry :: {retry, SojournTime}.
nb_ask(Broker) ->
    gen_fsm:sync_send_event(Broker, nb_ask, infinity).

%% @doc Tries to match with a process calling `ask/1' on the same broker but
%% does not enqueue the request if no immediate match.
%%
%% @see nb_ask/1
-spec nb_ask_r(Broker) -> Go | Retry when
      Broker :: broker(),
      Go :: {go, Ref, Pid, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      SojournTime :: 0,
      Retry :: {retry, SojournTime}.
nb_ask_r(Broker) ->
    gen_fsm:sync_send_event(Broker, nb_bid, infinity).

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask_r/1'. Returns `{await, Tag, Process}'.
%%
%% `Tag' is a monitor `reference()' that uniquely identifies the reply
%% containing the result of the request. `Process', is the pid (`pid()') or
%% process name (`{atom(), node()}') of the monitored broker. To cancel the
%% request call `cancel(Process, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Pid, SojournTime}' or
%% `{Tag, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. A process making
%% multiple requests can reuse the monitor reference for subsequent requests to
%% the same broker process (`Process') using `async_ask/2'.
%%
%% @see cancel/2
%% @see async_ask/2
-spec async_ask(Broker) -> {await, Tag, Process} when
      Broker :: broker(),
      Tag :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask(Broker) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, async_ask, [Broker]}});
        Broker2 ->
            Tag = monitor(process, Broker2),
            %% Will use {self(), Tag} as the From term from a sync call. This
            %% means that a reply is sent to self() in form {Tag, Reply}.
            gen_fsm:send_event(Broker2, {ask, {self(), Tag}}),
            {await, Tag, Broker2}
    end.

%% @doc Sends an asynchronous request to match with a process calling `ask_r/1'.
%% Returns `{await, Tag, Process}'.
%%
%% `Tag' is a `any()' that identifies the reply containing the result of the
%% request. `Process', is the pid (`pid()') or process name (`{atom(), node()}')
%% of the broker. To cancel all requests identified by `Tag' on broker
%% `Process' call `cancel(Process, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Pid, SojournTime}' or
%% `{Tag, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. If the broker
%% exits or is on a disconnected node there is no guarantee of a reply and so
%% the caller should take appropriate steps to handle this scenario.
%%
%% @see cancel/2
-spec async_ask(Broker, Tag) -> {await, Tag, Process} when
      Broker :: broker(),
      Tag :: any(),
      Process :: pid() | {atom(), node()}.
async_ask(Broker, Tag) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, async_ask, [Broker]}});
        Broker2 ->
            gen_fsm:send_event(Broker2, {ask, {self(), Tag}}),
            {await, Tag, Broker2}
    end.

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask/1'.
%%
%% @see async_ask/1
%% @see cancel/2
-spec async_ask_r(Broker) -> {await, Tag, Process} when
      Broker :: broker(),
      Tag :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask_r(Broker) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, async_ask_r, [Broker]}});
        Broker2 ->
            Tag = monitor(process, Broker2),
            gen_fsm:send_event(Broker2, {bid, {self(), Tag}}),
            {await, Tag, Broker2}
    end.

%% @doc Sends an asynchronous request to match with a process calling `ask/1'.
%%
%% @see async_ask/2
%% @see cancel/2
-spec async_ask_r(Broker, Tag) -> {await, Tag, Process} when
      Broker :: broker(),
      Tag :: any(),
      Process :: pid() | {atom(), node()}.
async_ask_r(Broker, Tag) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, async_ask_r, [Broker]}});
        Broker2 ->
            gen_fsm:send_event(Broker2, {bid, {self(), Tag}}),
            {await, Tag, Broker2}
    end.

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
      Go :: {go, Ref, Pid, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
await(Tag, Timeout) ->
    receive
        {Tag, {go, _, _, _} = Reply} ->
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
-spec cancel(Broker, Tag) -> Count | false when
      Broker :: broker(),
      Tag :: any(),
      Count :: pos_integer().
cancel(Broker, Tag) ->
    gen_fsm:sync_send_event(Broker, {cancel, Tag}).

%% @doc Change the configuration of the broker. Returns `ok' on success and
%% `{error, Reason}' on failure, where `Reason', is the reason for failure.
%%
%% Broker calls the `init/1' callback to get the new configuration. If `init/1'
%% returns `ignore' the config does not change.
%%
%% @see start_link/2
-spec change_config(Broker, Timeout) -> ok | {error, Reason} when
      Broker :: broker(),
      Timeout :: timeout(),
      Reason :: any().
change_config(Broker, Timeout) ->
    gen_fsm:sync_send_all_state_event(Broker, change_config, Timeout).

%% @doc Starts a broker with callback module `Module' and argument `Args'.
%%
%% The callback modules implements one callback, `init/1', with single argument
%% `Args'. `init/1' should return
%% `{ok, {AskQueueSpec, AskRQueueSpec, Interval})' or `ignore'. `AskQueuSpec' is
%% the queue specification for the `ask' queue and `AskRQueueSpec' is the queue
%% specification for the `ask_r' queue. `Interval' is the internval in
%% milliseconds that the active queue is polled. This ensures that the active
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
-spec start_link(Module, Args) -> StartReturn when
      Module :: module(),
      Args :: any(),
      StartReturn :: start_return().
start_link(Module, Args) ->
    gen_fsm:start_link(?MODULE, {Module, Args}, []).

%% @doc Starts a broker with name `Name', callback module `Module' and argument
%% `Args'.
%%
%% @see start_link/2
-spec start_link(Name, Module, Args) -> StartReturn when
      Name :: name(),
      Module :: module(),
      Args :: any(),
      StartReturn :: start_return().
start_link(Name, Module, Args) ->
    gen_fsm:start_link(Name, ?MODULE, {Module, Args}, []).

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
bidding({bid, Bid}, State) ->
    bidding_bid(Bid, State);
bidding({ask, Ask}, State) ->
    bidding_ask(Ask, State);
bidding({timeout, TRef, sbroker_timer}, State) when is_reference(TRef) ->
    {next_state, bidding, bidding_timeout(TRef, State)}.

%% @private
bidding(bid, Bid, State) ->
    bidding_bid(Bid, State);
bidding(nb_bid, Bid, State) ->
    retry(Bid),
    {next_state, bidding, State};
bidding(ask, Ask, State) ->
    bidding_ask(Ask, State);
bidding(nb_ask, Ask, State) ->
    bidding_nb_ask(Ask, State);
bidding({cancel, Tag}, _From, State) ->
    {Reply, NState} = bidding_cancel(Tag, State),
    {reply, Reply, bidding, NState}.

%% @private
asking({bid, Bid}, State) ->
    asking_bid(Bid, State);
asking({ask, Ask}, State) ->
    asking_ask(Ask, State);
asking({timeout, TRef, sbroker_timer}, State) when is_reference(TRef) ->
    {next_state, asking, asking_timeout(TRef, State)}.

%% @private
asking(bid, Bid, State) ->
    asking_bid(Bid, State);
asking(nb_bid, Bid, State) ->
    asking_nb_bid(Bid, State);
asking(ask, Ask, State) ->
    asking_ask(Ask, State);
asking(nb_ask, Ask, State) ->
    retry(Ask),
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

%% test api

%% @hidden
force_timeout(Broker) ->
    gen_fsm:send_event(Broker, {timeout, make_ref(), sbroker_timer}).

%% Internal

init(Module, Args, AskQueueSpec, AskRQueueSpec, Interval) ->
    {Time, Timer} = sbroker_timer:start(Interval),
    A = sbroker_queue:new(Time, AskQueueSpec),
    B = sbroker_queue:new(Time, AskRQueueSpec),
    State = #state{module=Module, args=Args, timer=Timer, asking=A, bidding=B},
    {ok, bidding, State}.

bidding_bid(Bid, #state{timer=Timer, bidding=B} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    NB = sbroker_queue:in(Time, Bid, B),
    {next_state, bidding, State#state{timer=NTimer, bidding=NB}}.

bidding_ask(Ask, #state{timer=Timer, bidding=B, asking=A} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    case sbroker_queue:out(Time, B) of
        {{SojournTime, {MRef, Bid}}, NB} ->
            settle(MRef, Bid, SojournTime, Ask, 0),
            {next_state, bidding, State#state{timer=NTimer, bidding=NB}};
        {empty, NB} ->
            NA = sbroker_queue:in(Time, Ask, A),
            NState = State#state{timer=NTimer, bidding=NB, asking=NA},
            {next_state, asking, NState}
    end.

bidding_nb_ask(Ask, #state{timer=Timer, bidding=B} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    case sbroker_queue:out(Time, B) of
        {{SojournTime, {MRef, Bid}}, NB} ->
            settle(MRef, Bid, SojournTime, Ask, 0),
            {next_state, bidding, State#state{timer=NTimer, bidding=NB}};
        {empty, NB} ->
            retry(Ask),
            {next_state, asking, State#state{timer=NTimer, bidding=NB}}
    end.

asking_ask(Ask, #state{timer=Timer, asking=A} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    NA = sbroker_queue:in(Time, Ask, A),
    {next_state, asking, State#state{timer=NTimer, asking=NA}}.

asking_bid(Bid, #state{timer=Timer, asking=A, bidding=B} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    case sbroker_queue:out(Time, A) of
        {{SojournTime, {MRef, Ask}}, NA} ->
            settle(MRef, Bid, 0, Ask, SojournTime),
            {next_state, asking, State#state{timer=NTimer, asking=NA}};
        {empty, NA} ->
            NB = sbroker_queue:in(Time, Bid, B),
            NState = State#state{timer=NTimer, asking=NA, bidding=NB},
            {next_state, bidding, NState}
    end.

asking_nb_bid(Bid, #state{timer=Timer, asking=A} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    case sbroker_queue:out(Time, A) of
        {{SojournTime, {MRef, Ask}}, NA} ->
            settle(MRef, Bid, 0, Ask, SojournTime),
            {next_state, asking, State#state{timer=NTimer, asking=NA}};
        {empty, NA} ->
            retry(Bid),
            {next_state, bidding, State#state{timer=NTimer, asking=NA}}
    end.

settle(MRef, {PidB, _} = Bid, SojournTimeB, {PidA, _} = Ask, SojournTimeA) ->
    %% Bid notified always messaged first.
    gen_fsm:reply(Bid, {go, MRef, PidA, SojournTimeB}),
    gen_fsm:reply(Ask, {go, MRef, PidB, SojournTimeA}),
    demonitor(MRef, [flush]).

retry(From) ->
    gen_fsm:reply(From, {retry, 0}).

bidding_cancel(Tag, #state{timer=Timer, bidding=B} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    {Reply, NB} = sbroker_queue:cancel(Time, Tag, B),
    {Reply, State#state{timer=NTimer, bidding=NB}}.

asking_cancel(Tag, #state{timer=Timer, asking=A} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    {Reply, NA} = sbroker_queue:cancel(Time, Tag, A),
    {Reply, State#state{timer=NTimer, asking=NA}}.

bidding_timeout(TRef, #state{timer=Timer, bidding=B} = State) ->
    {Time, NTimer} = sbroker_timer:timeout(TRef, Timer),
    NB = sbroker_queue:timeout(Time, B),
    State#state{timer=NTimer, bidding=NB}.

asking_timeout(TRef, #state{timer=Timer, asking=A} = State) ->
    {Time, NTimer} = sbroker_timer:timeout(TRef, Timer),
    NA = sbroker_queue:timeout(Time, A),
    State#state{timer=NTimer, asking=NA}.

bidding_down(MRef, #state{timer=Timer, bidding=B} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    NB = sbroker_queue:down(Time, MRef, B),
    State#state{timer=NTimer, bidding=NB}.

asking_down(MRef, #state{timer=Timer, asking=A} = State) ->
    {Time, NTimer} = sbroker_timer:read(Timer),
    NA = sbroker_queue:down(Time, MRef, A),
    State#state{timer=NTimer, asking=NA}.

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
    {_, NTimer} = sbroker_timer:config_change(Interval, Timer),
    State#state{timer=NTimer, asking=NA, bidding=NB}.
