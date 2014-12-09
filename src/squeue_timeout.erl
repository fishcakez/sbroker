%% @doc Implements a basic queue management algorithm where items are dropped
%% once their sojourn time is greater than a timeout value.
%%
%% `squeue_timeout' can be used as the active queue management module in a
%% `squeue' queue. It's argument is a `pos_integer()', which is the timeout
%% value, i.e. the minimum sojourn time at which items are dropped from the
%% queue.
-module(squeue_timeout).

-behaviour(squeue).

-export([init/1]).
-export([handle_timeout/3]).
-export([handle_out/3]).
-export([handle_join/3]).

-record(state, {timeout :: pos_integer(),
                timeout_next = 0 :: non_neg_integer()}).

%% @private
-spec init(Timeout) -> State when
      Timeout :: pos_integer(),
      State :: #state{}.
init(Timeout) when is_integer(Timeout) andalso Timeout > 0 ->
    #state{timeout=Timeout}.

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
      Q :: queue:queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(),
      NState :: #state{}.
-endif.
handle_timeout(Time, Q, #state{timeout_next=TimeoutNext} = State)
  when Time < TimeoutNext ->
    {[], Q, State};
handle_timeout(Time, Q, #state{timeout=Timeout} = State) ->
    timeout(queue:peek(Q), Time - Timeout, Time, Q, State, []).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_out(Time, Q, State) -> {[], Q, State} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: #state{}.
-else.
-spec handle_out(Time, Q, State) -> {[], Q, State} when
      Time :: non_neg_integer(),
      Q :: queue:queue(),
      State :: #state{}.
-endif.
handle_out(_Time, Q, State) ->
    {[], Q, State}.

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
handle_join(_Time, Q, State) ->
    case queue:is_empty(Q) of
        true ->
            {[], Q, State#state{timeout_next=0}};
        false ->
            {[], Q, State}
    end.

timeout(empty, _MinStart, Time, Q, #state{timeout=Timeout} = State, Drops) ->
    %% If an item is added immediately the first time it (or any item) could be
    %% dropped is in timeout.
    {Drops, Q, State#state{timeout_next=Time+Timeout}};
timeout({value, {Start, _}}, MinStart, _Time, Q,
        #state{timeout=Timeout} = State, Drops) when Start > MinStart ->
    %% Item is below sojourn timeout, it is the first item that can be
    %% dropped and it can't be dropped until it is above sojourn timeout.
    {Drops, Q, State#state{timeout_next=Start+Timeout}};
timeout({value, Item}, MinStart, Time, Q, State, Drops) ->
    %% Item is above sojourn timeout so drop it.
    NQ = queue:drop(Q),
    timeout(queue:peek(NQ), MinStart, Time, NQ, State, [Item | Drops]).
