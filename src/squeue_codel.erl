%% @doc Implements CoDel based roughly on Controlling Queue Delay, see
%% reference, with some changes.
%%
%% `squeue_codel' can be used as the active queue management module in a
%% `squeue' queue. It's arguments are of the form `{Target, Interval}', with
%% `Target', `pos_integer()', the target sojourn time of an item in the queue
%% and `Interval', `pos_integer()', the initial interval between drops once the
%% queue becomes slow.
%%
%% The follow differences apply compared to the classis CoDel algorithm:
%%
%% Interval commencing, dropping and changing to a non-dropping state can
%% occur on any function (not just when dequeuing) by inspecting the sojourn
%% time of the head (i.e. the oldest item) of the queue. This has the advantage
%% that dropping can occur as soon as possible rather than waiting for a
%% dequeue. More investigation needs to be done to ensure there are no ill
%% effects.
%%
%% The first change only occurs once per time change. This allows a queue-like
%% API. Note that this change is not as drastic as it appears because
%% the sojourn time of the head item can only decrease if the time has not
%% changed.
%%
%% The first change allows a LIFO queue as well as FIFO as the max sojourn
%% time of the queue is used to judge it's state, rather than the dequeued items
%% sojourn time.
%%
%% The time the queue became slow (i.e. time slow item entered queue + target
%% sojourn time + interval length) rather than the time the queue realised it
%% was slow and waited an interval (i.e. the current time + interval length) is
%% used for the time of first drop. This means that more than one drop can occur
%% when realising the queue has been slow for an interval.
%%
%% @reference Kathleen Nichols and Van Jacobson, Controlling Queue Delay,
%% ACM Queue, 6th May 2012.
-module(squeue_codel).

-behaviour(squeue).

-export([init/1]).
-export([handle_time/3]).
-export([handle_join/1]).

-record(config, {target :: pos_integer(),
                 interval :: pos_integer(),
                 count=0 :: non_neg_integer(),
                 drop_next=0 :: non_neg_integer()}).

-record(state, {config :: #config{},
                drop_first=infinity :: non_neg_integer() | infinity | dropping,
                timeout_next=0 :: non_neg_integer()}).

%% @private
-spec init({Target, Interval}) -> State when
      Target :: pos_integer(),
      Interval :: pos_integer(),
      State :: #state{}.
init({Target, Interval})
  when is_integer(Target) andalso Target > 0 andalso
       is_integer(Interval) andalso Interval > 0 ->
    #state{config=#config{target=Target, interval=Interval}}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_time(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: #state{}.
-else.
-spec handle_time(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: non_neg_integer(),
      Q :: queue:queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(),
      NState :: #state{}.
-endif.
handle_time(Time, Q, #state{timeout_next=TimeoutNext} = State)
  when Time < TimeoutNext ->
    {[], Q, State};
handle_time(Time, Q, #state{config=#config{target=Target}} = State) ->
    timeout(queue:peek(Q), Time - Target, Time, Q, State).

%% @private
-spec handle_join(State) -> NState when
      State :: #state{},
      NState :: #state{}.
handle_join(State) ->
    State#state{timeout_next=0}.

%% Empty queue so reset drop_first
timeout(empty, _MinStart, Time, Q,
        #state{config=#config{target=Target}} = State) ->
    %% The first time state can change is if an item is added immediately and
    %% remains in the queue for at least the target sojourn time.
    {[], Q, State#state{drop_first=infinity, timeout_next=Time+Target}};
%% Item currently below target sojourn time
timeout({value, {Start, _}}, MinStart, _Time, Q,
        #state{config=#config{target=Target}} = State) when Start > MinStart ->
    %% First time state can change is if this item remains in the queue over the
    %% target sojourn time.
    TimeoutNext = Start + Target,
    {[], Q, State#state{drop_first=infinity, timeout_next=TimeoutNext}};
%% Item is first above target sojourn time, begin first interval.
timeout({value, {Start, _} = Item}, MinStart, Time, Q,
             #state{drop_first=infinity,
                    config=#config{target=Target,
                                   interval=Interval}} = State) ->
    %% Unlike CoDel draft implementation the first drop point is the time at
    %% which the queue became slow, which may not be a point in the future. The
    %% DropFirst will then be used to create the DropNext should the queue
    %% remain slow
    case Start + Target + Interval of
        DropFirst when DropFirst > Time ->
            {[], Q, State#state{drop_first=DropFirst}};
        DropFirst ->
            NQ = queue:drop(Q),
            NState = drop_control(DropFirst, State),
            drops(queue:peek(NQ), MinStart, Time, NQ, NState, [Item])
    end;
%% Item above target sojourn time during a consecutive "slow" interval.
timeout(_Result, _MinStart, Time, Q,
          #state{drop_first=dropping,
                 config=#config{drop_next=DropNext}} = State)
  when DropNext > Time ->
    {[], Q, State};
%% Item above target sojourn time and is the last in a consecutive "slow"
%% interval.
timeout({value, Item}, MinStart, Time, Q,
        #state{drop_first=dropping} = State) ->
    NQ = queue:drop(Q),
    drops(queue:peek(NQ), MinStart, Time, NQ, State, [Item]);
%% Item above target sojourn time during the first "slow" interval.
timeout(_Result, _MinStart, Time, Q, #state{drop_first=DropFirst} = State)
  when DropFirst > Time ->
    {[], Q, State};
%% Item above target sojourn time and is the last item in the first "slow"
%% interval so drop it.
timeout({value, Item}, MinStart, Time, Q,
        #state{drop_first=DropFirst} = State) ->
    NQ = queue:drop(Q),
    NState = drop_control(DropFirst, State),
    drops(queue:peek(NQ), MinStart, Time, NQ, NState, [Item]).

drops(empty, _MinStart, Time, Q, #state{config=#config{target=Target}} = State,
      Drops) ->
    {Drops, Q, State#state{drop_first=infinity, timeout_next=Time+Target}};
drops({value, {Start, _}}, MinStart, _Time, Q,
      #state{config=#config{target=Target}} = State, Drops)
  when Start > MinStart ->
    {Drops, Q, State#state{drop_first=infinity, timeout_next=Start+Target}};
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
