%% @doc Implements CoDel based roughly on Controlling Queue Delay, see
%% reference.
%%
%% `squeue_codel' can be used as the active queue management module in a
%% `squeue' queue. It's arguments are of the form `{Target, Interval}', with
%% `Target', `pos_integer()', the target sojourn time of an item in the queue
%% and `Interval', `pos_integer()', the initial interval between drops once the
%% queue becomes slow.
%%
%% @reference Kathleen Nichols and Van Jacobson, Controlling Queue Delay,
%% ACM Queue, 6th May 2012.
-module(squeue_codel).

-behaviour(squeue).

-export([init/1]).
-export([handle_timeout/3]).
-export([handle_enqueue/3]).
-export([handle_dequeue/3]).
-export([handle_join/3]).

-record(config, {target :: pos_integer(),
                 interval :: pos_integer(),
                 count=0 :: non_neg_integer(),
                 drop_next=0 :: non_neg_integer()}).

-record(state, {config :: #config{},
                drop_first=infinity :: non_neg_integer() | infinity | dropping,
                out_next=0 :: non_neg_integer()}).

-opaque state() :: #state{}.

-export_type([state/0]).

%% @private
-spec init({Target, Interval}) -> State when
      Target :: pos_integer(),
      Interval :: pos_integer(),
      State :: state().
init({Target, Interval})
  when is_integer(Target) andalso Target > 0 andalso
       is_integer(Interval) andalso Interval > 0 ->
    #state{config=#config{target=Target, interval=Interval}}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_timeout(Time, Q, State) -> {[], Q, State} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: state().
-else.
-spec handle_timeout(Time, Q, State) -> {[], Q, State} when
      Time :: non_neg_integer(),
      Q :: queue:queue(),
      State :: state().
-endif.
handle_timeout(_Time, Q, State) ->
    {[], Q, State}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_enqueue(Time, Q, State) -> {[], Q, State} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: state().
-else.
-spec handle_enqueue(Time, Q, State) -> {[], Q, State} when
      Time :: non_neg_integer(),
      Q :: queue:queue(),
      State :: state().
-endif.
handle_enqueue(_Time, Q, State) ->
    {[], Q, State}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_dequeue(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: state(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: state().
-else.
-spec handle_dequeue(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue:queue(Item),
      State :: state(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: state().
-endif.
handle_dequeue(Time, Q, #state{out_next=OutNext} = State)
  when Time < OutNext ->
    {[], Q, State};
handle_dequeue(Time, Q, #state{config=#config{target=Target}} = State) ->
    out(queue:peek(Q), Time - Target, Time, Q, State).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_join(Time, Q, State) -> {[], Q, NState} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: state(),
      NState :: state().
-else.
-spec handle_join(Time, Q, State) -> {[], Q, NState} when
      Time :: non_neg_integer(),
      Q :: queue:queue(),
      State :: state(),
      NState :: state().
-endif.
handle_join(_Time, Q, State) ->
    case queue:is_empty(Q) of
        true ->
            {[], Q, State#state{out_next=0}};
        false ->
            {[], Q, State}
    end.

%% Empty queue so reset drop_first
out(empty, _MinStart, Time, Q, #state{config=#config{target=Target}} = State) ->
    %% The first time state can change is if an item is added immediately and
    %% remains in the queue for at least the target sojourn time.
    {[], Q, State#state{drop_first=infinity, out_next=Time+Target}};
%% Item currently below target sojourn time
out({value, {Start, _}}, MinStart, _Time, Q,
        #state{config=#config{target=Target}} = State) when Start > MinStart ->
    %% First time state can change is if this item remains in the queue over the
    %% target sojourn time.
    {[], Q, State#state{drop_first=infinity, out_next=Start+Target}};
%% Item is first above target sojourn time, begin first interval.
out(_Result, _MinStart, Time, Q,
    #state{drop_first=infinity, config=#config{interval=Interval}} = State) ->
    {[], Q, State#state{drop_first=Time + Interval}};
%% Item above target sojourn time during a consecutive "slow" interval.
out(_Result, _MinStart, Time, Q,
          #state{drop_first=dropping,
                 config=#config{drop_next=DropNext}} = State)
  when DropNext > Time ->
    {[], Q, State};
%% Item above target sojourn time and is the last in a consecutive "slow"
%% interval.
out({value, Item}, MinStart, Time, Q, #state{drop_first=dropping} = State) ->
    NQ = queue:drop(Q),
    drops(queue:peek(NQ), MinStart, Time, NQ, State, [Item]);
%% Item above target sojourn time during the first "slow" interval.
out(_Result, _MinStart, Time, Q, #state{drop_first=DropFirst} = State)
  when DropFirst > Time ->
    {[], Q, State};
%% Item above target sojourn time and is the last item in the first "slow"
%% interval so drop it.
out({value, Item}, MinStart, Time, Q, State) ->
    NQ = queue:drop(Q),
    NState = drop_control(Time, State),
    case queue:peek(NQ) of
        empty ->
            {[Item], NQ, NState#state{drop_first=infinity}};
        {value, {Start, _}} when Start > MinStart ->
            {[Item], NQ, NState#state{drop_first=infinity}};
        _ ->
            {[Item], NQ, NState}
    end.

drops(empty, _MinStart, Time, Q, #state{config=#config{target=Target}} = State,
      Drops) ->
    {Drops, Q, State#state{drop_first=infinity, out_next=Time+Target}};
drops({value, {Start, _}}, MinStart, _Time, Q,
      #state{config=#config{target=Target}} = State, Drops)
  when Start > MinStart ->
    {Drops, Q, State#state{drop_first=infinity, out_next=Start+Target}};
drops({value, Item}, MinStart, Time, Q,
      #state{config=#config{count=C, drop_next=DropNext}} = State, Drops) ->
    case drop_control(C+1, DropNext, State) of
        #state{config=#config{drop_next=NDropNext}} = NState
          when NDropNext > Time ->
            {Drops, Q, NState};
        NState ->
            NQ = queue:drop(Q),
            drops(queue:peek(NQ), MinStart, Time, NQ, NState, [Item | Drops])
    end.

%% If first "slow" item in "slow" interval was "soon" after switching from
%% dropping to not dropping use the previous dropping interval length as it
%% should be appropriate - as done in CoDel draft implemenation.
drop_control(Time, #state{config=#config{interval=Interval, count=C,
                                         drop_next=DropNext}} = State)
  when C > 2 andalso Time - DropNext < Interval ->
    drop_control(C - 2, Time, State);
drop_control(Time, State) ->
    drop_control(1, Time, State).

%% Shrink the interval to increase drop rate and reduce sojourn time.
drop_control(C, Time,
             #state{config=#config{interval=Interval} = Config} = State) ->
    DropNext = Time + trunc(Interval / math:sqrt(C)),
    NConfig = Config#config{count=C, drop_next=DropNext},
    State#state{config=NConfig, drop_first=dropping}.
