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
%% timeout.
%%
%% @reference Kathleen Nichols and Van Jacobson, Controlling Queue Delay,
%% ACM Queue, 6th May 2012.
-module(squeue_codel_timeout).

-behaviour(squeue).

-export([init/1]).
-export([handle_timeout/3]).
-export([handle_out/3]).
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
init({Target, Interval, Timeout}) ->
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
    timeout(queue:peek(Q), Time - Timeout, Time, Q, State, []).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_out(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: #state{}.
-else.
-spec handle_out(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue:queue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: #state{}.
-endif.
handle_out(Time, Q, #state{codel=Codel} = State) ->
    {Drops, NQ, NCodel} = squeue_codel:handle_out(Time, Q, Codel),
    {Drops, NQ, State#state{codel=NCodel}}.

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
timeout(_Result, MinStart, Time, Q, #state{codel=Codel} = State, Drops) ->
    %% Item is above sojourn timeout so drop it. CoDel must be used to
    %% dequeue so that it's state is kept up to date.
    {Drops2, NQ, NCodel} = squeue_codel:handle_out(Time, Q, Codel),
    NDrops = Drops2 ++ Drops,
    NState = State#state{codel=NCodel},
    case queue:peek(NQ) of
        empty ->
            %% Previous item was dropped by CoDel.
            {NDrops, NQ, NState};
        {value, {Start, _}} when Start > MinStart ->
            %% Previous item was dropped by CoDel, don't drop this item
            %% though as it is below sojourn timeout.
            {NDrops, NQ, NState};
        {value, Item} ->
            %% Item is above sojourn timeout, drop it and try again.
            NQ2 = queue:drop(NQ),
            NDrops2 = [Item | NDrops],
            timeout(queue:peek(NQ2), MinStart, Time, NQ2, NState, NDrops2)
    end.
