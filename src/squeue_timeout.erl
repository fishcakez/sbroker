-module(squeue_timeout).

-behaviour(squeue).

-export([init/1]).
-export([handle_time/3]).
-export([handle_join/1]).

-record(state, {timeout :: pos_integer(),
                timeout_next = 0 :: non_neg_integer()}).

-spec init(Timeout) -> State when
      Timeout :: pos_integer(),
      State :: #state{}.
init(Timeout) when is_integer(Timeout) andalso Timeout > 0 ->
    #state{timeout=Timeout}.

-spec handle_time(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue:queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(),
      NState :: #state{}.
handle_time(Time, Q, #state{timeout_next=TimeoutNext} = State)
  when Time < TimeoutNext ->
    {[], Q, State};
handle_time(Time, Q, #state{timeout=Timeout} = State) ->
    timeout(queue:peek(Q), Time - Timeout, Time, Q, State, []).

-spec handle_join(State) -> NState when
      State :: #state{},
      NState :: #state{}.
handle_join(State) ->
    State#state{timeout_next=0}.

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
