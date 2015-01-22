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
-export([cancel/2]).

-export([start_link/0]).
-export([start_link/1]).
-export([start_link/3]).
-export([start_link/4]).

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
-type squeue() :: squeue:squeue().

-export_type([broker/0]).
-export_type([queue_spec/0]).

-record(config, {interval :: pos_integer(),
                 next_timeout = 0 :: non_neg_integer(),
                 timer = make_ref() :: reference()}).

-record(queue, {out :: out | out_r,
                drop_out :: out | out_r,
                size :: non_neg_integer() | infinity,
                len = 0 :: non_neg_integer(),
                squeue :: squeue()}).

-record(state, {config :: #config{},
                bidding :: #queue{squeue :: squeue()},
                asking :: #queue{squeue :: squeue()}}).

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
%% process calling `ask_r/1'. Returns `{await, ARef, Process}'.
%%
%% `ARef' is a monitor `reference()' that uniquely identifies the reply
%% containing the result of the request. `Process', is the pid (`pid()') or
%% process name (`{atom(), node()}') of the monitored broker. To cancel the
%% request call `cancel(Process, ARef)'.
%%
%% The reply is of the form `{ARef, {go, Ref, Pid, SojournTime}' or
%% `{ARef, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. A process making
%% multiple requests can reuse the monitor reference for subsequent requests to
%% the same broker process (`Process') using `async_ask/2'.
%%
%% @see cancel/2
%% @see async_ask/2
-spec async_ask(Broker) -> {await, ARef, Process} when
      Broker :: broker(),
      ARef :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask(Broker) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, async_ask, [Broker]}});
        Broker2 ->
            ARef = monitor(process, Broker2),
            %% Will use {self(), ARef} as the From term from a sync call. This
            %% means that a reply is sent to self() in form {ARef, Reply}.
            gen_fsm:send_event(Broker2, {ask, {self(), ARef}}),
            {await, ARef, Broker2}
    end.

%% @doc Sends an asynchronous request to match with a process calling `ask_r/1'.
%% Returns `{await, ARef, Process}'.
%%
%% `ARef' is a `reference()' that identifies the reply containing the result of
%% the request. `Process', is the pid (`pid()') or process name
%% (`{atom(), node()}') of the broker. To cancel all requests identified by
%% `ARef' on broker `Process' call `cancel(Process, ARef)'.
%%
%% The reply is of the form `{ARef, {go, Ref, Pid, SojournTime}' or
%% `{ARef, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. If the broker
%% exits or is on a disconnected node there is no guarantee of a reply and so
%% the caller should take appropriate steps to handle this scenario.
%%
%% @see cancel/2
-spec async_ask(Broker, ARef) -> {await, ARef, Process} when
      Broker :: broker(),
      ARef :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask(Broker, ARef) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, async_ask, [Broker]}});
        Broker2 ->
            gen_fsm:send_event(Broker2, {ask, {self(), ARef}}),
            {await, ARef, Broker2}
    end.

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask/1'.
%%
%% @see async_ask/1
%% @see cancel/2
-spec async_ask_r(Broker) -> {await, ARef, Process} when
      Broker :: broker(),
      ARef :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask_r(Broker) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, async_ask_r, [Broker]}});
        Broker2 ->
            ARef = monitor(process, Broker2),
            gen_fsm:send_event(Broker2, {bid, {self(), ARef}}),
            {await, ARef, Broker2}
    end.

%% @doc Sends an asynchronous request to match with a process calling `ask/1'.
%%
%% @see async_ask/2
%% @see cancel/2
-spec async_ask_r(Broker, ARef) -> {await, ARef, Process} when
      Broker :: broker(),
      ARef :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask_r(Broker, ARef) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, async_ask_r, [Broker]}});
        Broker2 ->
            gen_fsm:send_event(Broker2, {bid, {self(), ARef}}),
            {await, ARef, Broker2}
    end.

%% @doc Cancels an asynchronous request. Returns `ok' on success and
%% `{error, not_found}' if the request does not exist. In the later case a
%% caller may wish to check its message queue for an existing reply.
%%
%% @see async_ask/1
%% @see async_ask_r/1
-spec cancel(Broker, ARef) -> ok | {error, not_found} when
      Broker :: broker(),
      ARef :: reference().
cancel(Broker, ARef) ->
    gen_fsm:sync_send_event(Broker, {cancel, ARef}).

%% @doc Starts a broker with default queues. The default queue uses
%% `squeue_timeout' with a timeout of `5000', which means that items are dropped
%% if they spend longer than 5000ms in the queue. The queue has a size of
%% `infinity' and uses `out' to dequeue items. The tick interval is `200', so
%% the active queue management timeout strategy is applied at least every 200ms.
-spec start_link() -> StartReturn when
    StartReturn :: start_return().
start_link() ->
    Spec = {squeue_timeout, 5000, out, infinity, drop},
    start_link(Spec, Spec, 200).

%% @doc Starts a registered broker with default queues.
%% @see start_link/1
-spec start_link(Name) -> StartReturn when
      Name :: name(),
      StartReturn :: start_return().
start_link(Name) ->
    Spec = {squeue_timeout, 5000, out, infinity, drop},
    start_link(Name, Spec, Spec, 200).

%% @doc Starts a broker with custom queues.
%%
%% The first argument, `AskQueueSpec', is the queue specification for the `ask'
%% queue. Processes that call `ask/1' (or `async_ask/1') can not be matched with
%% a process calling `ask_r/1' (or `async_ask_r/1') will be queued in this
%% queue. Similarly, the second argument, `AskRQueueSpec', is the queue
%% specification for `ask_r' queue. Processes that call `ask_r/1' (or
%% `async_ask_r/1') are enqueued in this queue. The third argument, `Interval',
%% is the interval in milliseconds that the active queue is polled. This ensures
%% that the active queue management strategy is applied even if no processes are
%% enqueued/dequeued.
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
-spec start_link(AskQueueSpec, AskRQueueSpec, Interval) -> StartReturn when
      AskQueueSpec :: queue_spec(),
      AskRQueueSpec :: queue_spec(),
      Interval :: pos_integer(),
      StartReturn :: start_return().
start_link(AskQueueSpec, AskRQueueSpec, Interval) ->
    gen_fsm:start_link(?MODULE, {AskQueueSpec, AskRQueueSpec, Interval}, []).

%% @doc Starts a registered broker with custom queues.
%% @see start_link/3
-spec start_link(Name, AskQueueSpec, AskRQueueSpec, Interval) ->
    StartReturn when
      Name :: name(),
      AskQueueSpec :: queue_spec(),
      AskRQueueSpec :: queue_spec(),
      Interval :: pos_integer(),
      StartReturn :: start_return().
start_link(Name, AskQueueSpec, AskRQueueSpec, Interval) ->
    gen_fsm:start_link(Name, ?MODULE, {AskQueueSpec, AskRQueueSpec, Interval},
                       []).

%% gen_fsm api

%% Inside the gen_fsm an ask_r request is refered to as a bid to make the
%% difference between ask and ask_r clearer.

%% @private
init({AskQueueSpec, AskRQueueSpec, Interval}) ->
    Config = #config{interval=Interval},
    State = #state{config=start_timer(Config), asking=new(AskQueueSpec),
                   bidding=new(AskRQueueSpec)},
    {ok, bidding, State}.

%% @private
bidding({bid, Bid}, State) ->
    bidding_bid(Bid, State);
bidding({ask, Ask}, State) ->
    bidding_ask(Ask, State);
bidding({timeout, TRef, ?MODULE},
        #state{config=#config{timer=TRef, next_timeout=Time}} = State) ->
    {next_state, bidding, bidding_timeout(Time, State)};
bidding({timeout, _, ?MODULE}, State) ->
    {next_state, bidding, State}.

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
bidding({cancel, ARef}, _From, State) ->
    {Reply, NState} = bidding_cancel(ARef, State),
    {reply, Reply, bidding, NState}.

%% @private
asking({bid, Bid}, State) ->
    asking_bid(Bid, State);
asking({ask, Ask}, State) ->
    asking_ask(Ask, State);
asking({timeout, TRef, ?MODULE},
       #state{config=#config{timer=TRef, next_timeout=Time}} = State) ->
    {next_state, asking, asking_timeout(Time, State)};
asking({timeout, _, ?MODULE}, State) ->
    {next_state, asking, State}.

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
asking({cancel, ARef}, _From, State) ->
    {Reply, NState} = asking_cancel(ARef, State),
    {reply, Reply, asking, NState}.

%% @private
handle_event(Event, _, State) ->
    {stop, {bad_event, Event}, State}.

%% @private
handle_sync_event(force_timeout, _, StateName,
             #state{config=#config{timer=TRef}} = State) ->
    _ = erlang:cancel_timer(TRef),
    gen_fsm:send_event(self(), {timeout, TRef, ?MODULE}),
    {reply, ok, StateName, State};
handle_sync_event(Event, _, _, State) ->
    {stop, {bad_event, Event}, State}.

%% @private
handle_info({'DOWN', MRef, _, _, _}, bidding, State) ->
    {next_state, bidding, bidding_filter(MRef, State)};
handle_info({'DOWN', MRef, _, _, _}, asking, State) ->
    {next_state, asking, asking_filter(MRef, State)};
handle_info(Msg, StateName, State) ->
    error_logger:error_msg("sbroker ~p received unexpected message: ~p~n",
                           [self(), Msg]),
    {next_state, StateName, State}.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% @private
terminate(_Reason, _StateName, _State) ->
    ok.

%% test api

%% @hidden
force_timeout(Broker) ->
    gen_fsm:sync_send_all_state_event(Broker, force_timeout).

%% Internal

start_timer(#config{interval=Interval, next_timeout=NextTimeout} = Config) ->
    TRef = gen_fsm:start_timer(Interval, ?MODULE),
    Config#config{next_timeout=NextTimeout+Interval, timer=TRef}.

read_timer(#config{next_timeout=NextTimeout, timer=TRef} = Config) ->
    case erlang:read_timer(TRef) of
        false ->
            {start_timer(Config), NextTimeout};
        Rem ->
            {Config, NextTimeout-Rem}
    end.

new({Mod, Args, Out, Size, Drop})
  when (Out =:= out orelse Out =:= out_r) andalso
       ((is_integer(Size) andalso Size >= 0) orelse Size =:= infinity) ->
    S=squeue:new(Mod, Args),
    #queue{out=Out, drop_out=drop_out(Drop), size=Size, squeue=S}.

drop_out(drop) -> out;
drop_out(drop_r) -> out_r.

bidding_bid(Bid, #state{config=Config, bidding=B} = State) ->
    {NConfig, Time} = read_timer(Config),
    NState = State#state{config=NConfig, bidding=in(Time, Bid, B)},
    {next_state, bidding, NState}.

bidding_ask(Ask, #state{config=Config, bidding=B, asking=A} = State) ->
    {NConfig, Time} = read_timer(Config),
    case out(Time, B) of
        {{SojournTime, {MRef, Bid}}, NB} ->
            settle(MRef, Bid, SojournTime, Ask, 0),
            {next_state, bidding, State#state{config=NConfig, bidding=NB}};
        {empty, NB} ->
            NA = in(Time, Ask, A),
            NState = State#state{config=NConfig, bidding=NB, asking=NA},
            {next_state, asking, NState}
    end.

bidding_nb_ask(Ask, #state{config=Config, bidding=B} = State) ->
    {NConfig, Time} = read_timer(Config),
    case out(Time, B) of
        {{SojournTime, {MRef, Bid}}, NB} ->
            settle(MRef, Bid, SojournTime, Ask, 0),
            {next_state, bidding, State#state{config=NConfig, bidding=NB}};
        {empty, NB} ->
            retry(Ask),
            {next_state, asking, State#state{config=NConfig, bidding=NB}}
    end.

asking_ask(Ask, #state{config=Config, asking=A} = State) ->
    {NConfig, Time} = read_timer(Config),
    NState = State#state{config=NConfig, asking=in(Time, Ask, A)},
    {next_state, asking, NState}.

asking_bid(Bid, #state{config=Config, asking=A, bidding=B} = State) ->
    {NConfig, Time} = read_timer(Config),
    case out(Time, A) of
        {{SojournTime, {MRef, Ask}}, NA} ->
            settle(MRef, Bid, 0, Ask, SojournTime),
            {next_state, asking, State#state{config=NConfig, asking=NA}};
        {empty, NA} ->
            NB = in(Time, Bid, B),
            NState = State#state{config=NConfig, asking=NA, bidding=NB},
            {next_state, bidding, NState}
    end.

asking_nb_bid(Bid, #state{config=Config, asking=A} = State) ->
    {NConfig, Time} = read_timer(Config),
    case out(Time, A) of
        {{SojournTime, {MRef, Ask}}, NA} ->
            settle(MRef, Bid, 0, Ask, SojournTime),
            {next_state, asking, State#state{config=NConfig, asking=NA}};
        {empty, NA} ->
            retry(Bid),
            {next_state, bidding, State#state{config=NConfig, asking=NA}}
    end.

settle(MRef, {PidB, _} = Bid, SojournTimeB, {PidA, _} = Ask, SojournTimeA) ->
    %% Bid notified always messaged first.
    gen_fsm:reply(Bid, {go, MRef, PidA, SojournTimeB}),
    gen_fsm:reply(Ask, {go, MRef, PidB, SojournTimeA}).

retry(From) ->
    gen_fsm:reply(From, {retry, 0}).

in(Time, {Pid, _} = From,
   #queue{drop_out=DropOut, size=Size, len=Len, squeue=S} = Q) ->
    MRef = monitor(process, Pid),
    {Drops, NS} = squeue:in(Time, {MRef, From}, S),
    case Len - drops(Drops) + 1 of
        NLen when NLen > Size ->
            {Dropped2, NS2} = drop_out(DropOut, NS),
            Q#queue{len=NLen-Dropped2, squeue=NS2};
        NLen ->
            Q#queue{len=NLen, squeue=NS}
    end.

drop_out(DropOut, Q) ->
    case squeue:DropOut(Q) of
        {empty, Drops, NQ} ->
            {drops(Drops), NQ};
        {Item, Drops, NQ} ->
            {drops(Drops) + drops([Item]), NQ}
    end.

drops(Items) ->
    drops(Items, 0).

drops([{SojournTime, {MRef, From}} | Rest], N) ->
    demonitor(MRef, [flush]),
    gen_fsm:reply(From, {drop, SojournTime}),
    drops(Rest, N+1);
drops([], N) ->
    N.

out(Time, #queue{out=Out, len=Len, squeue=S} = Q) ->
    case squeue:Out(Time, S) of
        {empty, Drops, NS2} ->
            {empty, Q#queue{len=Len-drops(Drops), squeue=NS2}};
        {{_, {MRef, _}} = Item, Drops, NS2} ->
            %% It might be nice to use info here also but that would be awkward
            %% to test.
            demonitor(MRef, [flush]),
            {Item, Q#queue{len=Len-drops(Drops)-1, squeue=NS2}}
    end.

bidding_cancel(ARef, #state{config=Config, bidding=B} = State) ->
    {NConfig, Time} = read_timer(Config),
    {Reply, NB} = cancel(Time, ARef, B),
    {Reply, State#state{config=NConfig, bidding=NB}}.

asking_cancel(ARef, #state{config=Config, asking=A} = State) ->
    {NConfig, Time} = read_timer(Config),
    {Reply, NA} = cancel(Time, ARef, A),
    {Reply, State#state{config=NConfig, asking=NA}}.

cancel(Time, ARef, #queue{len=Len, squeue=S} = Q) ->
    Cancel = fun({MRef, {_, ARef2}}) when ARef2 =:= ARef ->
                     demonitor(MRef, [flush]),
                     false;
                (_) ->
                     true
             end,
    {Drops, NS} = squeue:filter(Time, Cancel, S),
    Dropped = drops(Drops),
    NLen = squeue:len(NS),
    NQ = Q#queue{len=NLen, squeue=NS},
    case Len - Dropped of
        %% No items filtered, i.e. the async request not in queue
        NLen ->
            {{error, not_found}, NQ};
        _ ->
            {ok, NQ}
    end.

bidding_timeout(Time, #state{config=Config, bidding=B} = State) ->
    State#state{config=start_timer(Config), bidding=timeout(Time, B)}.

asking_timeout(Time, #state{config=Config, asking=A} = State) ->
    State#state{config=start_timer(Config), asking=timeout(Time, A)}.

timeout(Time, #queue{len=Len, squeue=S} = Q) ->
    {Drops, NS} = squeue:timeout(Time, S),
    Q#queue{len=Len-drops(Drops), squeue=NS}.

bidding_filter(MRef, #state{config=Config, bidding=B} = State) ->
    {NConfig, Time} = read_timer(Config),
    State#state{config=NConfig, bidding=filter(Time, MRef, B)}.

asking_filter(MRef, #state{config=Config, asking=A} = State) ->
    {NConfig, Time} = read_timer(Config),
    State#state{config=NConfig, asking=filter(Time, MRef, A)}.

filter(Time, MRef, #queue{squeue=S} = Q) ->
    {Drops, NS} = squeue:filter(Time, fun({MRef2, _}) -> MRef2 =/= MRef end, S),
    _ = drops(Drops),
    Q#queue{len=squeue:len(NS), squeue=NS}.
