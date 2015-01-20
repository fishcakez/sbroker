%% @doc Implements CoDel based roughly on Controlling Queue Delay, see
%% reference, with the additional that items over a timeout sojourn time
%% are dequeued using CoDel.
%%
%% `squeue_codel' can be used as the active queue management module in a
%% `squeue' queue. It's arguments are of the form `{Target, Interval, Timeout}',
%% with `Target', `pos_integer()', the target sojourn time of an item in the
%% queue; `Interval', `pos_integer()', the initial interval between drops once
%% the queue becomes slow; `Timeout', `pos_integer()', the timeout value, i.e.
%% the minimum sojourn time at which items are dropped from the queue due to a
%% timeout. `Timeout' must be greater than `Target'.
%%
%% @reference Kathleen Nichols and Van Jacobson, Controlling Queue Delay,
%% ACM Queue, 6th May 2012.
-module(squeue_codel_timeout).

-behaviour(squeue).

-export([init/1]).
-export([handle_timeout/3]).
-export([handle_enqueue/3]).
-export([handle_dequeue/3]).
-export([handle_join/3]).

-record(state, {timeout :: pos_integer(),
                codel :: squeue_codel:state(),
                timeout_next = 0 :: non_neg_integer()}).

%% @private
-spec init({Target, Interval, Timeout}) -> State when
      Target :: pos_integer(),
      Interval :: pos_integer(),
      Timeout :: pos_integer(),
      State :: #state{}.
init({Target, Interval, Timeout}) when Target < Timeout ->
    #state{timeout=Timeout, codel=squeue_codel:init({Target, Interval})}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_timeout(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: #state{}.
-else.
-spec handle_timeout(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue:queue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: #state{}.
-endif.
handle_timeout(Time, Q, #state{timeout_next=TimeoutNext} = State)
  when Time < TimeoutNext ->
    {[], Q, State};
handle_timeout(Time, Q, #state{timeout=Timeout} = State) ->
    timeout(queue:peek(Q), Time - Timeout, Time, Q, State).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_enqueue(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: #state{}.
-else.
-spec handle_enqueue(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue:queue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: #state{}.
-endif.
handle_enqueue(Time, Q, State) ->
    handle_timeout(Time, Q, State).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_dequeue(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: #state{}.
-else.
-spec handle_dequeue(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue:queue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: #state{}.
-endif.
handle_dequeue(Time, Q, #state{timeout_next=TimeoutNext, codel=Codel} = State)
  when Time < TimeoutNext ->
    {Drops, NQ, NCodel} = squeue_codel:handle_dequeue(Time, Q, Codel),
    {Drops, NQ, State#state{codel=NCodel}};
handle_dequeue(Time, Q, #state{timeout=Timeout, codel=Codel} = State) ->
    MinStart = Time - Timeout,
    case queue:peek(Q) of
        empty ->
            {[], NQ, NCodel} = squeue_codel:handle_dequeue(Time, Q, Codel),
            TimeoutNext = Time + Timeout,
            {[], NQ, State#state{codel=NCodel, timeout_next=TimeoutNext}};
        {value, {Start, _}} when Start > MinStart ->
            {Drops, NQ, NCodel} = squeue_codel:handle_dequeue(Time, Q, Codel),
            TimeoutNext = Start + Timeout,
            {Drops, NQ, State#state{codel=NCodel, timeout_next=TimeoutNext}};
        Result ->
            {Drops, NQ, NState} = timeout(Result, MinStart, Time, Q, State),
            #state{codel=NCodel} = NState,
            {Drops2, NQ2, NCodel2} = squeue_codel:handle_dequeue(Time, NQ, NCodel),
            {Drops2 ++ Drops, NQ2, NState#state{codel=NCodel2}}
    end.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_join(Time, Q, State) -> {[], Q, NState} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: #state{},
      NState :: #state{}.
-else.
-spec handle_join(Time, Q, State) -> {[], Q, NState} when
      Time :: non_neg_integer(),
      Q :: queue:queue(),
      State :: #state{},
      NState :: #state{}.
-endif.
handle_join(Time, Q, #state{codel=Codel} = State) ->
    {[], NQ, NCodel} = squeue_codel:handle_join(Time, Q, Codel),
    case queue:is_empty(NQ) of
        true ->
            {[], NQ, State#state{codel=NCodel, timeout_next=0}};
        false ->
            {[], NQ, State#state{codel=NCodel}}
    end.

timeout(empty, _MinStart, Time, Q, #state{timeout=Timeout} = State) ->
    %% If an item is added immediately the first time it (or any item) could be
    %% dropped due to a timeout is in timeout.
    {[], Q, State#state{timeout_next=Time+Timeout}};
timeout({value, {Start, _}}, MinStart, _Time, Q,
        #state{timeout=Timeout} = State) when Start > MinStart ->
    %% Item is below sojourn timeout, it is the first item that can be
    %% dropped due to a timeout and it can't be dropped due to a timeout until
    %% it is above sojourn timeout.
    {[], Q, State#state{timeout_next=Start+Timeout}};
timeout(_Result, MinStart, Time, Q, #state{codel=Codel} = State) ->
    %% Item is above sojourn timeout so drop it. CoDel must be used to
    %% dequeue so that it's state is kept up to date.
    {Drops, NQ, NCodel} = squeue_codel:handle_dequeue(Time, Q, Codel),
    NState = State#state{codel=NCodel},
    %% CoDel can only drop once per Time and so if CoDel is in dropping
    %% mode it can't drop anymore. The only reason CoDel would change
    %% its state is if an item with a sojourn below target, reset it to
    %% non-dropping or an item above set it start tracking a first slow
    %% interval. As the timeout must be greater than target
    %% (otherwise CoDel does not executed) then CoDel will not change
    %% its state if only items with a sojourn time of timeout or greater
    %% are dequeued. Also the queue must be on its first slow interval,
    %% or a later one as a item with a sojourn time above target (>=
    %% timeout) has been dequeued.
    timeout(queue:peek(NQ), MinStart, Time, NQ, NState, Drops).

timeout(empty, _MinStart, Time, Q, #state{timeout=Timeout} = State, Drops) ->
    %% If an item is added immediately the first time it (or any item) could be
    %% dropped due to a timeout is in timeout.
    {Drops, Q, State#state{timeout_next=Time+Timeout}};
timeout({value, {Start, _}}, MinStart, _Time, Q,
        #state{timeout=Timeout} = State, Drops) when Start > MinStart ->
    %% Item is below sojourn timeout, it is the first item that can be
    %% dropped due to a timeout and it can't be dropped due to a timeout until
    %% it is above sojourn timeout.
    {Drops, Q, State#state{timeout_next=Start+Timeout}};
timeout({value, Item}, MinStart, Time, Q, State, Drops) ->
    NQ = queue:drop(Q),
    timeout(queue:peek(NQ), MinStart, Time, NQ, State, [Item | Drops]).
