-module(squeue).

-compile({no_auto_import, [get/1]}).

%% Original API

-export([new/0]).
-export([new/1]).
-export([new/2]).
-export([new/3]).
-export([is_queue/1]).
-export([is_queue/2]).
-export([is_empty/1]).
-export([is_empty/2]).
-export([len/1]).
-export([len/2]).
-export([in/2]).
-export([in/3]).
-export([in_r/2]).
-export([in_r/3]).
-export([out/1]).
-export([out/2]).
-export([out_r/1]).
-export([out_r/2]).
-export([from_list/1]).
-export([from_list/2]).
-export([from_list/3]).
-export([from_list/4]).
-export([to_list/1]).
-export([to_list/2]).
-export([reverse/1]).
-export([reverse/2]).
-export([split/2]).
-export([split/3]).
-export([join/2]).
-export([join/3]).
-export([filter/2]).
-export([filter/3]).
-export([member/2]).
-export([member/3]).

%% Additional API

-export([time/1]).
-export([timeout/2]).

%% Extended API

-export([get/1]).
-export([get/2]).
-export([get_r/1]).
-export([get_r/2]).
-export([peek/1]).
-export([peek/2]).
-export([peek_r/1]).
-export([peek_r/2]).
-export([drop/1]).
-export([drop/2]).
-export([drop_r/1]).
-export([drop_r/2]).

%% Okasaki API

-export([cons/2]).
-export([cons/3]).
-export([head/1]).
-export([head/2]).
-export([tail/1]).
-export([tail/2]).
-export([snoc/2]).
-export([snoc/3]).
-export([daeh/1]).
-export([daeh/2]).
-export([last/1]).
-export([last/2]).
-export([liat/1]).
-export([liat/2]).
-export([init/1]).
-export([init/2]).

%% Misspelt Okasaki API

-export([lait/1]).
-export([lait/2]).

%% Test API

-export([from_start_list/4]).

-record(squeue, {module :: module(),
                 state :: any(),
                 time = 0 :: non_neg_integer(),
                 queue = queue:new() :: queue:queue()}).

-type squeue() :: squeue(any()).
-opaque squeue(Item) ::
    #squeue{queue :: queue:queue({non_neg_integer(), Item})}.

-export_type([squeue/0]).
-export_type([squeue/1]).

-callback init(Args :: any()) -> State :: any().
-callback handle_time(Time :: non_neg_integer(), Q :: queue:queue(),
                      State :: any()) ->
    {Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
     NQ :: queue:queue(), NState :: any()}.
-callback handle_join(State :: any()) -> NState :: any().

%% This module provides an API very similar to OTP's queue module. When the
%% function/arity is consistent between modules the behaviour is the same with
%% ONE exception: When a queue function's return contains {value, Item}, squeue
%% replaces the atom value with SojournTime, i.e. {SojournTime, Item}, where
%% SojournTime is the sojourn time of Item.
%%
%% In addition to the queue API, squeue provides a function with an extra first
%% argument for every function. This extra first argument is Time, of type
%% non_neg_integer(), which represents the current time of the queue with
%% no specific unit and defaults to 0. An squeue queue can not go backwards in
%% time so once a time has been set all subsequent calls must have the same or a
%% later time. If time decreases a function fails with badtime. If subsequent
%% calls are made without the Time argument, it is assumed that time has not
%% changed. The Time argument is used to calculate the sojourn time
%% (SojournTime) for each item in the queue.
%%
%% The following additional badtime errors may occur:
%%
%% reverse/2,3 will fail with badtime if all items in the queue were not added
%% at the same time.
%%
%% in_r/2,3 will fail with badtime if the head of the queue was not added at
%% the same time.
%%
%% join/2 will fail with badtime if both queues do not have the same time but
%% join/3 will not unless either queue has a time greater than Time (first
%% argument).
%%
%% Note that if Time is never advanced then a badtime error can not occur.
%%
%% Functions which take a Time argument and return a queue will also return a
%% list of dropped items and their sojourn time, i.e: [{SojournTime, Item}]. The
%% head of the list will be the item from the head of the queue (i.e. the oldest
%% item in the queue) and so has the longest sojourn time in the list. If a
%% function without the Time argument just returns a queue, the verion with a
%% Time argument will return: {DroppedList, Queue}. Similarly when a function
%% without the Time argument returns empty or an Item with a queue:
%% {{SojournTime, Item}, DroppedList, Queue} or two queues:
%% {DroppedList, Queue1, Queue2}.
%%
%% In the case of is_queue/is_empty/member/peek/get functions a DroppedList
%% isn't returned, but the returned values is based on any items having been
%% dropped.
%%
%% Note that items can only be dropped when time advances, if the Time argument
%% is included but time has not advanced no items are dropped. This occurs
%% because the sojourn time of the item at the head of the queue has not
%% increased.
%%
%% By default no items are dropped. An active queue management algorithm can be
%% used with new/2,3 or from_list/3,4. Two such algorithms are included:
%% squeue_timeout and squeue_codel, along with squeue_naive which does not drop
%% any items (the default algorithm) which takes any term for argument.
%%
%% squeue_timeout is a simple alogrithm that drops all items that have a sojourn
%% time greater than or equal to a timeout. The timeout is configured
%% with argument Timeout. Note:
%%
%% * An infinity timeout is not supported, this behaviour is equivalent to
%% squeue_naive
%%
%% * A timeout of 0 is not allowed because items are not dropped until time
%% advances. A timeout of 0 would imply the item should be dropped on the next
%% call on the queue. If a timeout of 0 was allowed it would not be obeyed and
%% produce the same behaviour as a timeout of 1. This restriction allows squeue
%% to mirror the queue API (with the value/SojournTime expection). It also makes
%% the API easier to use as dropped items only need to be handled once per
%% unique Time.
%%
%% squeue_codel implements the CoDel alogrithm with some changes made so that
%% the queue's status changing/dropping can occur on any function - not just on
%% dequeuing. The CoDel alorithm detects when items are spending longer than a
%% target sojourn time in the queue and drops an item with decreasing intervals
%% until the target sojourn time is met. The initial interval and target sojourn
%% time are configurable with argument {Target, Interval}. Note:
%%
%% * A target of 0 is not allowed for the same reasons as a timeout of 0 is not
%% allowed for squeue_timeout.
%%
%% * An interval of 0 is not allowed but is equivalent to using squeue_timeout
%% with a Timeout of Target.
%%
%% * A Target or Interval of infinity is not allowed, the behaviour of either is
%% equivalent to squeue_naive.
%%
%%
%% In the case of get/2, get_r/2, drop/2 and drop_r/2 the queue management
%% algorithm is carried out before trying to get/drop the item, and this item is
%% not included in the drop list in case of drop/2,drop_r/2. Note therefore that
%% these functions will fail with reason empty if is_empty/2 returns true, even
%% if is_empty/1 returns false.
%%
%% Additional API:
%%
%% time/1 returns the queues current time
%% timeout/2 applies the queue manegement alogrithm to the queue if Time (first
%% argument) advances.

%% Original API

-spec new() -> S when
      S :: squeue().
new() ->
    new(squeue_naive, undefined).

-spec new(Time) -> {Drops, S} when
      Time :: non_neg_integer(),
      Drops :: [],
      S :: squeue().
new(Time) ->
    new(Time, squeue_naive, undefined).

-spec new(Module, Args) -> S when
      Module :: module(),
      Args :: any(),
      S :: squeue().
new(Module, Args) ->
    #squeue{module=Module, state=Module:init(Args)}.

-spec new(Time, Module, Args) -> {Drops, S} when
      Time :: non_neg_integer(),
      Module :: module(),
      Args :: any(),
      Drops :: [],
      S :: squeue().
new(Time, Module, Args) when is_integer(Time) andalso Time >= 0 ->
    State = Module:init(Args),
    {[], #squeue{module=Module, time=Time, state=State}}.

-spec is_queue(Term) -> Bool when
      Term :: any(),
      Bool :: boolean().
is_queue(#squeue{}) ->
    true;
is_queue(_Other) ->
    false.

%% Just for completeness.
-spec is_queue(Time, Term) -> Bool when
      Time :: non_neg_integer(),
      Term :: any(),
      Bool :: boolean().
is_queue(Time, #squeue{time=PrevTime})
  when is_integer(Time) andalso Time >= PrevTime ->
    true;
is_queue(Time, #squeue{} = S) ->
    error(badtime, [Time, S]);
is_queue(_Time, _Other) ->
    false.

-spec is_empty(S) -> Bool when
      S :: squeue(),
      Bool :: boolean().
is_empty(#squeue{queue=Q}) ->
    queue:is_empty(Q).

-spec is_empty(Time, S) -> Bool when
      Time :: non_neg_integer(),
      S :: squeue(),
      Bool :: boolean().
is_empty(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_time(Time, Q, State),
    queue:is_empty(NQ);
is_empty(Time, #squeue{time=Time} = S) ->
    is_empty(S);
is_empty(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec len(S) -> Len when
      S :: squeue(),
      Len :: non_neg_integer().
len(#squeue{queue=Q}) ->
    queue:len(Q).

-spec len(Time, S) -> Len when
      Time :: non_neg_integer(),
      S :: squeue(),
      Len :: non_neg_integer().
len(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_time(Time, Q, State),
    queue:len(NQ);
len(Time, #squeue{time=Time} = S) ->
    len(S);
len(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec in(Item, S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
in(Item, #squeue{time=Time, queue=Q} = S) ->
    S#squeue{queue=queue:in({Time, Item}, Q)}.

-spec in(Time, Item, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
in(Time, Item, #squeue{module=Module, state=State, time=PrevTime, queue=Q} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    NS = S#squeue{time=Time, queue=queue:in({Time, Item}, NQ), state=NState},
    {sojourn_drops(Time, Drops), NS};
in(Time, Item, #squeue{time=Time} = S) ->
    {[], in(Item, S)};
in(Time, Item, #squeue{} = S) ->
    error(badtime, [Time, Item, S]).

-spec in_r(Item, S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
in_r(Item, #squeue{time=Time, queue=Q} = S) ->
    %% in_r/2,3 is awkward as item at front of queue must have oldest start
    %% time. Therefore only allow adding to head if queue is empty of head has
    %% same time. This means that if Time is always 0 that in_r always succeeds.
    case queue:peek(Q) of
        empty ->
            S#squeue{queue=queue:in_r({Time, Item}, Q)};
        {value, {Time, _}} ->
            S#squeue{queue=queue:in_r({Time, Item}, Q)};
        _ ->
            error(badtime, [Item, S])
    end.

-spec in_r(Item, Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
in_r(Time, Item,
     #squeue{module=Module, state=State, time=PrevTime, queue=Q} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    NS = in_r(Item, S#squeue{time=Time, queue=NQ, state=NState}),
    {sojourn_drops(Time, Drops), NS};
in_r(Time, Item, #squeue{time=Time} = S) ->
    {[], in_r(Item, S)};
in_r(Time, Item, #squeue{} = S) ->
    error(badtime, [Time, Item, S]).

-spec out(S) -> {Result, NS} when
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      NS :: squeue(Item).
out(#squeue{time=Time, queue=Q} = S) ->
    case queue:out(Q) of
        {empty, NQ} ->
            {empty, S#squeue{queue=NQ}};
        {{value, Result}, NQ} ->
            {sojourn_time(Time, Result), S#squeue{queue=NQ}}
    end.

-spec out(Time, S) -> {Result, Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    case queue:out(NQ) of
        {empty, NQ2} ->
            {empty, Drops2, S#squeue{time=Time, queue=NQ2, state=NState}};
        {{value, Result}, NQ2} ->
            NS = S#squeue{time=Time, queue=NQ2, state=NState},
            {sojourn_time(Time, Result), Drops2, NS}
    end;
out(Time, #squeue{time=Time} = S) ->
    {Result, NS} = out(S),
    {Result, [], NS};
out(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec out_r(S) -> {Result, NS} when
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      NS :: squeue(Item).
out_r(#squeue{time=Time, queue=Q} = S) ->
    case queue:out_r(Q) of
        {empty, NQ} ->
            {empty, S#squeue{queue=NQ}};
        {{value, Result}, NQ} ->
            {sojourn_time(Time, Result), S#squeue{queue=NQ}}
    end.

-spec out_r(Time, S) -> {Result, Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    case queue:out_r(NQ) of
        {empty, NQ2} ->
            {empty, Drops2, S#squeue{time=Time, queue=NQ2, state=NState}};
        {{value, Result}, NQ2} ->
            Result2 = sojourn_time(Time, Result),
            {Result2, Drops2, S#squeue{time=Time, queue=NQ2, state=NState}}
    end;
out_r(Time, #squeue{time=Time} = S) ->
    {Result, NS} = out_r(S),
    {Result, [], NS};
out_r(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec from_list(List) -> S when
      List :: [Item],
      S :: squeue(Item).
from_list(List) ->
    from_list(List, squeue_naive, undefined).

-spec from_list(Time, List) -> {Drop, S} when
      Time :: non_neg_integer(),
      List :: [Item],
      Drop :: [],
      S :: squeue(Item).
from_list(Time, List) ->
    from_list(Time, List, squeue_naive, undefined).

-spec from_list(List, Module, Args) -> S when
      List :: [Item],
      Module :: module(),
      Args :: any(),
      S :: squeue(Item).
from_list(List, Module, Args) ->
    {[], S} = from_list(0, List, Module, Args),
    S.

-spec from_list(Time, List, Module, Args) -> {Drops, S} when
      Time :: non_neg_integer(),
      List :: [Item],
      Module :: module(),
      Args :: any(),
      Drops :: [],
      S :: squeue(Item).
from_list(Time, List, Module, Args) when is_integer(Time) andalso Time >= 0 ->
    Q = queue:from_list([{Time, Item} || Item <- List]),
    State = Module:init(Args),
    {[], #squeue{module=Module, time=Time, queue=Q, state=State}}.

-spec to_list(S) -> List when
      S :: squeue(Item),
      List :: [Item].
to_list(#squeue{queue=Q}) ->
    [Item || {_Start, Item} <- queue:to_list(Q)].

-spec to_list(Time, S) -> List when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      List :: [Item].
to_list(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_time(Time, Q, State),
    [Item || {_Start, Item} <- queue:to_list(NQ)];
to_list(Time, #squeue{time=Time} = S) ->
    to_list(S);
to_list(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec reverse(S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
reverse(#squeue{queue=Q} = S) ->
    case {queue:peek(Q), queue:peek_r(Q)} of
        %% All InTime's must be the same for reverse to maintain order.
        {{value, {InTime1, _}}, {value, {InTime2, _}}}
          when InTime1 =/= InTime2 ->
            error(badtime, [S]);
        _Other ->
            NQ = queue:reverse(Q),
            S#squeue{queue=NQ}
    end.

-spec reverse(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
reverse(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    {Drops2, reverse(S#squeue{time=Time, queue=NQ, state=NState})};
reverse(Time, #squeue{time=Time} = S) ->
    {[],  reverse(S)};
reverse(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec split(N, S) -> {S1, S2} when
      N :: non_neg_integer(),
      S :: squeue(Item),
      S1 :: squeue(Item),
      S2 :: squeue(Item).
split(N, #squeue{queue=Q} = S) ->
    {Q1, Q2} = queue:split(N, Q),
    {S#squeue{queue=Q1}, S#squeue{queue=Q2}}.

-spec split(Time, N, S) -> {Drops, S1, S2} when
      Time :: non_neg_integer(),
      N :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      S1 :: squeue(Item),
      S2 :: squeue(Item).
split(Time, N, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    {S1, S2} = split(N, S#squeue{time=Time, queue=NQ, state=NState}),
    {sojourn_drops(Time, Drops), S1, S2};
split(Time, N, #squeue{time=Time} = S) ->
    {S1, S2} = split(N, S),
    {[], S1, S2};
split(Time, N, #squeue{} = S) ->
    error(badtime, [Time, N, S]).

-spec join(S1, S2) -> NS when
      S1 :: squeue(Item),
      S2 :: squeue(Item),
      NS :: squeue(Item).
%% To merge two queues they must have the same Time.
join(#squeue{module=Module1, time=Time, queue=Q1, state=State1} = S1,
     #squeue{time=Time, queue=Q2} = S2) ->
    case {queue:peek_r(Q1), queue:peek(Q2)} of
        {{value, {TailStart1, _}}, {value, {HeadStart2, _}}}
          when TailStart1 > HeadStart2 ->
            %% queues contain overlapping start times.
            error(badtime, [S1, S2]);
        _ ->
            NQ = queue:join(Q1, Q2),
            %% handle_join/1 is required to notify the queue manager that the
            %% max sojourn time of the queue may have increased (though only if
            %% the head queue was empty).
            S1#squeue{queue=NQ, state=Module1:handle_join(State1)}
    end;
join(#squeue{} = S1, #squeue{} = S2) ->
    error(badtime, [S1, S2]).

-spec join(Time, S1, S2) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S1 :: squeue(Item),
      S2 :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
join(Time, #squeue{time=Time} = S1, #squeue{time=Time} = S2) ->
    {[], join(S1, S2)};
join(Time, #squeue{module=Module1, time=Time1, queue=Q1, state=State1} = S1,
     #squeue{module=Module2, time=Time2, queue=Q2, state=State2} = S2)
  when is_integer(Time) andalso Time > Time1 andalso Time > Time2 ->
    {Drops1, NQ1, NState1} = Module1:handle_time(Time, Q1, State1),
    NS1 = S1#squeue{time=Time, queue=NQ1, state=NState1},
    {Drops2, NQ2, NState2} = Module2:handle_time(Time, Q2, State2),
    NS2 = S2#squeue{time=Time, queue=NQ2, state=NState2},
    DropSort = fun({SojournT1, _Item1}, {SojournT2, _Item2}) ->
                       SojournT1 >= SojournT2
               end,
    Drops3 = sojourn_drops(Time, Drops1) ++ sojourn_drops(Time,  Drops2),
    Drops4 = lists:sort(DropSort, Drops3),
    {Drops4, join(NS1, NS2)};
join(Time1, #squeue{time=Time1} = S1,
     #squeue{module=Module2, time=Time2, queue=Q2, state=State2} = S2)
  when Time1 > Time2 ->
    {Drops, NQ2, NState2} = Module2:handle_time(Time1, Q2, State2),
    NS2 = S2#squeue{time=Time1, queue=NQ2, state=NState2},
    {sojourn_drops(Time1, Drops), join(S1, NS2)};
join(Time2, #squeue{module=Module1, time=Time1, queue=Q1, state=State1} = S1,
     #squeue{time=Time2} = S2) when Time2 > Time1 ->
    {Drops, NQ1, NState1} = Module1:handle_time(Time2, Q1, State1),
    NS1 = S1#squeue{time=Time2, queue=NQ1, state=NState1},
    {sojourn_drops(Time2, Drops), join(NS1, S2)};
join(Time, #squeue{} = S1, #squeue{} = S2) ->
    error(badtime, [Time, S1, S2]).

-spec filter(Filter, S) -> NS when
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      S :: squeue(Item),
      NS :: squeue(Item).
filter(Filter, #squeue{queue=Q} = S) ->
    NQ = queue:filter(make_filter(Filter), Q),
    S#squeue{queue=NQ}.

-spec filter(Time, Filter, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
filter(Time, Filter,
       #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    NQ2 = queue:filter(make_filter(Filter), NQ),
    {sojourn_drops(Time, Drops), S#squeue{time=Time, queue=NQ2, state=NState}};
filter(Time, Filter, #squeue{time=Time} = S) ->
    {[], filter(Filter, S)};
filter(Time, Filter, #squeue{} = S) ->
    error(badtime, [Time, Filter, S]).

-spec member(Item, S) -> Bool when
      S :: squeue(Item),
      Bool :: boolean().
member(Item, #squeue{queue=Q}) ->
    NQ = queue:filter(fun({_InTime, Item2}) -> Item2 =:= Item end, Q),
    not queue:is_empty(NQ).

-spec member(Time, Item, S) -> Bool when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Bool :: boolean().
member(Time, Item,
       #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_time(Time, Q, State),
    NQ2 = queue:filter(fun({_InTime, Item2}) -> Item2 =:= Item end, NQ),
    not queue:is_empty(NQ2);
member(Time, Item, #squeue{time=Time} = S) ->
    member(Item, S);
member(Time, Item, #squeue{} = S) ->
    error(badtime, [Time, Item, S]).

%% Additional API

%% Returns the time of the queue.
-spec time(S) -> Time when
      S :: squeue(),
      Time :: non_neg_integer().
time(#squeue{time=Time}) ->
    Time.

%% Sets the time of the queue.
-spec timeout(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
timeout(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    {sojourn_drops(Time, Drops), S#squeue{time=Time, queue=NQ, state=NState}};
timeout(Time, #squeue{time=Time} = S) ->
    {[], S};
timeout(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% Extended API

-spec get(S) -> Item when
      S :: squeue(Item).
get(#squeue{queue=Q}) ->
    {_InTime, Item} = queue:get(Q),
    Item.

-spec get(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
get(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_time(Time, Q, State),
    {_InTime, Item} = queue:get(NQ),
    Item;
get(Time, #squeue{time=Time} = S) ->
    get(S);
get(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec get_r(S) -> Item when
      S :: squeue(Item).
get_r(#squeue{queue=Q}) ->
    {_InTime, Item} = queue:get_r(Q),
    Item.

-spec get_r(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
get_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_time(Time, Q, State),
    {_InTime, Item} = queue:get_r(NQ),
    Item;
get_r(Time, #squeue{time=Time} = S) ->
    get_r(S);
get_r(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec peek(S) -> Item when
      S :: squeue(Item).
peek(#squeue{time=Time, queue=Q}) ->
    case queue:peek(Q) of
        empty ->
            empty;
        {value, Result} ->
            sojourn_time(Time, Result)
    end.

-spec peek(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
peek(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_time(Time, Q, State),
    case queue:peek(NQ) of
        empty ->
            empty;
        {value, Result} ->
            sojourn_time(Time, Result)
    end;
peek(Time, #squeue{time=Time} = S) ->
    peek(S);
peek(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec peek_r(S) -> Item when
      S :: squeue(Item).
peek_r(#squeue{time=Time, queue=Q}) ->
    case queue:peek_r(Q) of
        empty ->
            empty;
        {value, Result} ->
            sojourn_time(Time, Result)
    end.

-spec peek_r(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
peek_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_time(Time, Q, State),
    case queue:peek_r(NQ) of
        empty ->
            empty;
        {value, Result} ->
            sojourn_time(Time, Result)
    end;
peek_r(Time, #squeue{time=Time} = S) ->
    peek_r(S);
peek_r(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec drop(S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
drop(#squeue{queue=Q} = S) ->
    S#squeue{queue=queue:drop(Q)}.

-spec drop(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
drop(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    NQ2 = queue:drop(NQ),
    {sojourn_drops(Time, Drops), S#squeue{time=Time, queue=NQ2, state=NState}};
drop(Time, #squeue{time=Time} = S) ->
    {[], drop(S)};
drop(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

-spec drop_r(S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
drop_r(#squeue{queue=Q} = S) ->
    S#squeue{queue=queue:drop_r(Q)}.

-spec drop_r(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
drop_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_time(Time, Q, State),
    NQ2 = queue:drop_r(NQ),
    {sojourn_drops(Time, Drops), S#squeue{time=Time, queue=NQ2, state=NState}};
drop_r(Time, #squeue{time=Time} = S) ->
    {[], drop_r(S)};
drop_r(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% Okasaki API

-spec cons(Item, S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
cons(Item, S) ->
    in(Item, S).

-spec cons(Time, Item, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
cons(Time, Item, S) ->
    in(Time, Item, S).

-spec head(S) -> Item when
      S :: squeue(Item).
head(S) ->
    get(S).

-spec head(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
head(Time, S) ->
    get(Time, S).

-spec tail(S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
tail(S) ->
    drop(S).

-spec tail(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
tail(Time, S) ->
    drop(Time, S).

-spec snoc(Item, S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
snoc(Item, S) ->
    in_r(Item, S).

-spec snoc(Item, Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
snoc(Time, Item, S) ->
    in_r(Time, Item, S).

-spec daeh(S) -> Item when
      S :: squeue(Item).
daeh(S) ->
    get_r(S).

-spec daeh(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
daeh(Time, S) ->
    get_r(Time, S).

-spec last(S) -> Item when
      S :: squeue(Item).
last(S) ->
    get_r(S).

-spec last(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
last(Time, S) ->
    get_r(Time, S).

-spec liat(S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
liat(S) ->
    drop_r(S).

-spec liat(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
liat(Time, S) ->
    drop_r(Time, S).

-spec init(S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
init(S) ->
    drop_r(S).

-spec init(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
init(Time, S) ->
    drop_r(Time, S).

%% Misspelt Okasaki API

-spec lait(S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
lait(S) ->
    drop_r(S).

-spec lait(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
lait(Time, S) ->
    drop_r(Time, S).

%% Test API

from_start_list(Time, List, Module, Args) ->
    {[], S} = new(Time, Module, Args),
    S#squeue{queue=queue:from_list(List)}.

%% Internal

%% Note this reverses the list so oldest (front of queue) is first.
sojourn_drops(Time, Drops) ->
    sojourn_drops(Time, Drops, []).

sojourn_drops(_Time, [], Acc) ->
    Acc;
sojourn_drops(Time, [Drop | Drops], Acc) ->
    sojourn_drops(Time, Drops, [sojourn_time(Time, Drop) | Acc]).

sojourn_time(Time, {InTime, Item}) ->
    {Time - InTime, Item}.

make_filter(Filter) ->
    fun({InTime, Item}) ->
            case Filter(Item) of
                List when is_list(List) ->
                    [{InTime, Item2} || Item2 <- List];
                Bool when is_boolean(Bool) ->
                    Bool
            end
    end.
