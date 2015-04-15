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
%% This module provides sojourn-time based active queue management using
%% `squeue', with a feedback loop that dequeues items based on the sojourn times
%% of items in another queue.
%%
%% `new/0' and `new/1' create a new `svalve' with the default feedback loop
%% `svalve_naive' using an empty `squeue' managed by `squeue_naive'.
%% `svalve_naive' will never trigger a dequeue due to a signal.`new/2' and
%% `new/3' create a new `svalve' with a custom feedback loop, such as
%% `svalve_codel_r', using an empty `squeue' managed by `squeue_naive'.
%%
%% The underlying `squeue' queue can be retrieved and replaced with `squeue/1'
%% and `squeue/2'. `sojourn/2,3,4' signal the feedback loop to dequeue from the
%% head of the queue. `dropped/1,2,3' signal a drop and can also result in a
%% dequeue from the head. Alternatively `sojourn_r/2,3,4' and `dropped_r/1,2,3'
%% are used to dequeue from the tail.
%%
%% To close the valve `close/1', and prevent `sojourn/2,3,4',
%% `sojourn_r/2,3,4,', `dropped/1,2,3' and `dropped_r/1,2,3' from dequeuing
%% items. To open the valve `open/1', and allow items to be dequeued by the
%% feedback loop.
%%
%% All other functions are equivalent to those of the same name and arity in
%% `squeue'.
%%
%% `svalve' includes 3 feedback loop algorithms: `svalve_naive',
%% `svalve_timeout' and `svalve_codel_r'.
%%
%% A custom queue management algorithm must implement the `svalve' behaviour.
%% The first callback is `init/2':
%% ```
%% -callback init(Time :: integer(), Args :: any()) -> State :: any().
%% '''
%% `Time' is the time of the feedback loop, which may be less than the time of
%% the squeue. It is `0' for `new/2,3'. `Time' can be positive or negative. All
%% other callbacks will receive the current time of queue as the first argument.
%% It is monotonically increasing, so subsequent calls will use the same or a
%% greater time.
%%
%% `Args' is the last argument passed to `new/2' or `new/3'. It can be any term.
%%
%% The feedback loop is called for every signal sent to the queue and should
%% either dequeue an item with `squeue:out/1' or `squeue:out_r/1', or apply
%% active queue management with `squeue:timeout/1'. As the `squeue' uses a
%% different time the other variants should not be used.
%%
%% When the valve is open and a sojourn is signaled to dequeue from the head
%% using `sojourn/2,3,4' the callback function is `handle_sojourn/4':
%% ```
%% -callback handle_sojourn(Time ::integer(),
%%                          SojournTime :: non_neg_integer(),
%%                          S :: squeue:squeue(Item), State :: any()) ->
%%     {empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
%%      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
%%      NS :: squeue:squeue(Item), NState :: any()}.
%% '''
%% `SojournTime' is the time an item spent in another queue. `S' is the internal
%% `squeue' and `NS' is that `squeue' after dropping items and dequeuing (or
%% not) an item.
%%
%% `closed' means the feedback loop is closed. `empty' means the feedback loop
%% is open and would have dequeued an item if the queue was not empty.
%% `{ItemSojournTime, Item}' is the item and its sojourn time from the head of
%% the queue as returned by `squeue:out/1'.
%%
%% `State' is the state of the callback module, and `NState' is the new state
%% after handling the signal.
%%
%% `Drops' is a list of dropped items returned by `squeue:out/1' or
%% `squeue:timeout/1'.
%%
%% Similarly when the valve is open and a sojourn is signalled to dequeue from
%% the tail using `sojourn_r/2,3,4' the callback function is
%% `handle_sojourn_r/4':
%% ```
%% -callback handle_sojourn_r(Time ::integer(),
%%                            SojournTime :: non_neg_integer(),
%%                            S :: squeue:squeue(Item), State :: any()) ->
%%     {empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
%%      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
%%      NS :: squeue:squeue(Item), NState :: any()}.
%% '''
%% This callback should behave the same as `handle_sojourn/4' except use
%% `squeue:out_r/1' instead of `squeue:out/1'.
%%
%% When the valve is closed and a sojourn is signalled using either
%% `sojourn/2,3,4' or `sojourn_r/2,3,4' the callback function is
%% `handle_sojourn_closed/4':
%% ```
%% -callback handle_sojourn_closed(Time ::integer(),
%%                                 SojournTime :: non_neg_integer(),
%%                                 S :: squeue:squeue(Item), State :: any()) ->
%%     {closed, Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
%%      NS :: squeue:squeue(Item), NState :: any()}.
%% '''
%% As the valve is closed no items should be dequeued and active queue
%% management should be applied using `squeue:timeout/1'.
%%
%% Similarly when a drop is signalled, using `dropped/1,2,3' and
%% `dropped_r/1,2,3', except there is no `SojournTime' argument:
%% ```
%% -callback handle_dropped(Time ::integer(),
%%                          S :: squeue:squeue(Item), State :: any()) ->
%%     {empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
%%      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
%%      NS :: squeue:squeue(Item), NState :: any()}.
%% -callback handle_dropped_r(Time ::integer(),
%%                            S :: squeue:squeue(Item), State :: any()) ->
%%     {empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
%%      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
%%      NS :: squeue:squeue(Item), NState :: any()}.
%% -callback handle_dropped_closed(Time ::integer(),
%%                                 S :: squeue:squeue(Item), State :: any()) ->
%%     {closed, Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
%%      NS :: squeue:squeue(Item), NState :: any()}.
%% '''
%%
%% @see squeue
%% @see svalve_naive
%% @see svalve_timeout
%% @see svalve_codel_r
-module(svalve).

%% squeue API

-export([new/0]).
-export([new/1]).
-export([new/2]).
-export([new/3]).
-export([is_queue/1]).
-export([len/1]).
-export([in/2]).
-export([in/3]).
-export([in/4]).
-export([out/1]).
-export([out/2]).
-export([out_r/1]).
-export([out_r/2]).
-export([drop/1]).
-export([drop/2]).
-export([drop_r/1]).
-export([drop_r/2]).
-export([to_list/1]).
-export([join/2]).
-export([filter/2]).
-export([filter/3]).
-export([time/1]).
-export([time/2]).
-export([timeout/1]).
-export([timeout/2]).

%% Additional API

-export([open/1]).
-export([close/1]).
-export([squeue/1]).
-export([squeue/2]).
-export([sojourn/2]).
-export([sojourn/3]).
-export([sojourn/4]).
-export([sojourn_r/2]).
-export([sojourn_r/3]).
-export([sojourn_r/4]).
-export([dropped/1]).
-export([dropped/2]).
-export([dropped/3]).
-export([dropped_r/1]).
-export([dropped_r/2]).
-export([dropped_r/3]).

%% types

-record(svalve, {module :: module(),
                 state :: any(),
                 status = open :: open | closed,
                 time = 0 :: integer(),
                 squeue :: squeue:squeue()}).

-type svalve() :: svalve(any()).
-ifdef(LEGACY_TYPES).
-type svalve(Item) :: #svalve{squeue :: squeue:squeue(Item)}.
-else.
-opaque svalve(Item) :: #svalve{squeue :: squeue:squeue(Item)}.
-endif.

-export_type([svalve/0]).
-export_type([svalve/1]).

-callback init(Time :: integer(), Args :: any()) -> State :: any().
-callback handle_sojourn(Time ::integer(),
                         SojournTime :: non_neg_integer(),
                         S :: squeue:squeue(Item), State :: any()) ->
    {empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
     [{DropSojournTime :: non_neg_integer(), Item}], NS :: squeue:squeue(Item),
     NState :: any()}.
-callback handle_sojourn_r(Time ::integer(),
                           SojournTime :: non_neg_integer(),
                           S :: squeue:squeue(Item), State :: any()) ->
    {empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
     [{DropSojournTime :: non_neg_integer(), Item}], NS :: squeue:squeue(Item),
     NState :: any()}.
-callback handle_sojourn_closed(Time ::integer(),
                         SojournTime :: non_neg_integer(),
                         S :: squeue:squeue(Item), State :: any()) ->
    {closed, [{DropSojournTime :: non_neg_integer(), Item}],
     NS :: squeue:squeue(Item), NState :: any()}.
-callback handle_dropped(Time ::integer(),
                         S :: squeue:squeue(Item), State :: any()) ->
    {empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
     [{DropSojournTime :: non_neg_integer(), Item}], NS :: squeue:squeue(Item),
     NState :: any()}.
-callback handle_dropped_r(Time ::integer(),
                           S :: squeue:squeue(Item), State :: any()) ->
    {empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
     [{DropSojournTime :: non_neg_integer(), Item}], NS :: squeue:squeue(Item),
     NState :: any()}.
-callback handle_dropped_closed(Time ::integer(),
                                S :: squeue:squeue(Item), State :: any()) ->
    {closed, [{DropSojournTime :: non_neg_integer(), Item}],
     NS :: squeue:squeue(Item), NState :: any()}.

%% squeue API

%% @equiv new(0, svalve_naive, undefined)
-spec new() -> V when
      V :: svalve().
new() ->
    new(svalve_naive, undefined).

%% @equiv new(Time, svalve_naive, undefined)
-spec new(Time) -> V when
      Time :: integer(),
      V :: svalve().
new(Time) ->
    new(Time, svalve_naive, undefined).

%% @equiv new(0, Module, Args)
-spec new(Module, Args) -> V when
      Module :: module(),
      Args :: any(),
      V :: svalve().
new(Module, Args) ->
    new(0, Module, Args).

%% @doc Returns an empty queue, `V', with the `Module' feedback loop started
%% with arguments `Args', using an `squeue' managed by `squeue_naive' and time
%% of `Time'.
%% @see squeue:new/3
-spec new(Time, Module, Args) -> V when
      Time :: integer(),
      Module :: module(),
      Args :: any(),
      V :: svalve().
new(Time, Module, Args) ->
    #svalve{module=Module, time=Time, state=Module:init(Time, Args),
            squeue=squeue:new(Time)}.

%% @doc Tests if a term, `Term', is an `svalve' queue, returns `true' if is,
%% otherwise `false'.
%% @see squeue:is_queue/1
-spec is_queue(Term) -> Bool when
      Term :: any(),
      Bool :: boolean().
is_queue(#svalve{}) ->
    true;
is_queue(_Other) ->
    false.

%% @doc Returns the length of the queue, `V'.
%% @see squeue:len/1
-spec len(V) -> Len when
      V :: svalve(),
      Len :: non_neg_integer().
len(#svalve{squeue=S}) ->
    squeue:len(S).

%% @equiv in(time(V), time(V), Item, V)
-spec in(Item, V) -> {Drops, NV} when
      V :: svalve(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
in(Item, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:in(Item, S),
    {Drops, V#svalve{squeue=NS}}.

%% @equiv in(Time, Time, Item, V)
-spec in(Time, Item, V) -> {Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
in(Time, Item, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:in(Time, Item, S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and inserts the item, `Item',
%% with time `InTime' at the tail of queue, `V'.
%% @see squeue:in/4
-spec in(Time, InTime, Item, V) -> {Drops, NV} when
      Time :: integer(),
      InTime :: integer(),
      V :: svalve(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
in(Time, InTime, Item, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:in(Time, InTime, Item, S),
    {Drops, V#svalve{squeue=NS}}.

%% @equiv out(time(V), V)
-spec out(V) -> {Result, Drops, NV} when
      V :: svalve(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
out(#svalve{squeue=S} = V) ->
    {Result, Drops, NS} = squeue:out(S),
    {Result, Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and removes the item, `Item',
%% from the head of queue, `V'.
%% @see squeue:out/2
-spec out(Time, V) -> {Result, Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
out(Time, #svalve{squeue=S} = V) ->
    {Result, Drops, NS} = squeue:out(Time, S),
    {Result, Drops, V#svalve{squeue=NS}}.

%% @equiv out_r(time(V), V)
-spec out_r(V) -> {Result, Drops, NV} when
      V :: svalve(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
out_r(#svalve{squeue=S} = V) ->
    {Result, Drops, NS} = squeue:out_r(S),
    {Result, Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and removes the item, `Item',
%% from the tail of queue, `V'.
%% @see squeue:out/2
-spec out_r(Time, V) -> {Result, Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
out_r(Time, #svalve{squeue=S} = V) ->
    {Result, Drops, NS} = squeue:out_r(Time, S),
    {Result, Drops, V#svalve{squeue=NS}}.

%% @equiv drop(time(V), V)
-spec drop(V) -> {Drops, NV} when
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
drop(#svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:drop(S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and drops items, `Drops', from
%% the queue.
%% @see squeue:drop/2
-spec drop(Time, V) -> {Drops, NV} when
      Time :: non_neg_integer(),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
drop(Time, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:drop(Time, S),
    {Drops, V#svalve{squeue=NS}}.

%% @equiv drop_r(time(V), V)
-spec drop_r(V) -> {Drops, NV} when
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
drop_r(#svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:drop_r(S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and drops items, `Drops', from
%% the queue.
%% @see squeue:drop_r/2
-spec drop_r(Time, V) -> {Drops, NV} when
      Time :: non_neg_integer(),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
drop_r(Time, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:drop_r(Time, S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Returns a list of items, `List', in the queue, `V'.
%% @see squeue:to_list/1
-spec to_list(V) -> List when
      V :: svalve(Item),
      List :: [Item].
to_list(#svalve{squeue=S}) ->
    squeue:to_list(S).

%% @doc Joins two queues, `V1' and `V2', into one queue, `VS', with the items in
%% `V1' at the head and the items in `V2' at the tail.
%% @see squeue:join/2
-spec join(V1, V2) -> NV when
      V1 :: svalve(Item),
      V2 :: svalve(Item),
      NV :: svalve(Item).
join(#svalve{time=Time1, squeue=S1} = V1, #svalve{time=Time2, squeue=S2}) ->
    NS = squeue:join(S1, S2),
    V1#svalve{time=max(Time1, Time2), squeue=NS}.

%% @equiv filter(time(V), Filter, V)
-spec filter(Filter, V) -> {Drops, NV} when
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
filter(Filter, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:filter(Filter, S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time'  and applies a filter fun,
%% `Filter', to all items in the queue.
%% @see squeue:filter/3
-spec filter(Time, Filter, V) -> {Drops, NV} when
      Time :: integer(),
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
filter(Time, Filter, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:filter(Time, Filter, S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Returns the current time, `Time', of the queue, `V'.
%% @see squeue:time/1
-spec time(V) -> Time when
      V :: svalve(),
      Time :: integer().
time(#svalve{squeue=S}) ->
    squeue:time(S).

%% @doc Advances the queue, `V', to time `Time', without applying active queue
%% management.
%% @see squeue:time/2
-spec time(Time, V) -> NV when
      Time :: integer(),
      V :: svalve(Item),
      NV :: svalve(Item).
time(Time, #svalve{squeue=S} = V) ->
    V#svalve{squeue=squeue:time(Time, S)}.

%% @equiv timeout(time(V), V)
-spec timeout(V) -> {Drops, NV} when
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
timeout(#svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:timeout(S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time'.
%% @see squeue:timeout/2
-spec timeout(Time, V) -> {Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
timeout(Time, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {Drops, V#svalve{squeue=NS}}.

%% Additional API

%% @doc Get the internal `squeue' inside the queue, `V'.
-spec squeue(V) -> S when
      V :: svalve(Item),
      S :: squeue:squeue(Item).
squeue(#svalve{squeue=S}) ->
    S.

%% @doc Replace the internal `squeue' inside the queue, `V'.
%%
%% This function raises the error `badarg' if the current time of the new
%% squeue, `S', does not have the same time as the queue, `V'.
-spec squeue(S, V) -> NV when
      S :: squeue:squeue(Item),
      V :: svalve(),
      NV :: svalve(Item).
squeue(NS, #svalve{squeue=S} = V) ->
    case squeue:time(S) =:= squeue:time(NS) of
        true ->
            V#svalve{squeue=NS};
        _ ->
            error(badarg, [NS, V])
    end.

%% @doc Enable the feedback loop to dequeue items from queue, `V'.
-spec open(V) -> NV when
      V :: svalve(Item),
      NV :: svalve(Item).
open(V) ->
    V#svalve{status=open}.

%% @doc Disable the feedback loop from dequeuing items from queue, `V'.
-spec close(V) -> NV when
      V :: svalve(Item),
      NV :: svalve(Item).
close(V) ->
    V#svalve{status=closed}.

%% @equiv sojourn(time(V), time(V), SojournTime, V)
-spec sojourn(SojournTime, V) -> {Result, Drops, NV} when
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn(SojournTime, #svalve{squeue=S} = V) ->
    Time = squeue:time(S),
    sojourn(Time, Time, SojournTime, V).

%% @equiv sojourn(Time, Time, SojournTime, V)
-spec sojourn(Time, SojournTime, V) -> {Result, Drops, NV} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn(Time, SojournTime, V) ->
    sojourn(Time, Time, SojournTime, V).

%% @doc Advance time of the queue, `V', to `Time' and signal the sojourn time,
%% `SojournTime', at time `OutTime' of another queue to trigger a dequeue from
%% the head of the queue, `V'. Returns `{Result, Drops, NS}', where `Result'
%% is `closed' if the feedback loop is closed, `empty' if the feedback loop is
%% open but the queue is empty and `{SojournTime, Item}' if `Item' is dequeued
%% from the head of the queue. `SojournTime' is the time length of time `Item'
%% spent in the queue. `Drops' is the list of dropped items and their sojourn
%% times, and `NS' is the resulting queue without the removed and dropped items.
%%
%% If `OutTime' is less than the last time a sojourn time or drop was signalled
%% to the feedback loop, the sojourn time is signalled with the same time as the
%% last signal. This time may be before the current time of the queue.
%%
%% This function raises the error `badarg' if `OutTime' is greater than `Time'.
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
-spec sojourn(Time, OutTime, SojournTime, V) -> {Result, Drops, NV} when
      Time :: integer(),
      OutTime :: integer(),
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn(Time, OutTime, SojournTime, V) ->
    case handle_sojourn(handle_sojourn, Time, OutTime, SojournTime, V) of
        badarg ->
            error(badarg, [Time, OutTime, SojournTime, V]);
        Result ->
            Result
    end.

%% @equiv sojourn_r(time(V), time(V), SojournTime, V)
-spec sojourn_r(SojournTime, V) -> {Result, Drops, NV} when
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn_r(SojournTime, #svalve{squeue=S} = V) ->
    Time = squeue:time(S),
    sojourn_r(Time, Time, SojournTime, V).

%% @equiv sojourn_r(Time, Time, SojournTime, V)
-spec sojourn_r(Time, SojournTime, V) -> {Result, Drops, NV} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn_r(Time, SojournTime, V) ->
    sojourn_r(Time, Time, SojournTime, V).

%% @doc Advance time of the queue, `V', to `Time' and signal the sojourn time,
%% `SojournTime', at time `OutTime' of another queue to trigger a dequeue from
%% the tail of the queue, `V'.
%%
%% Except for dequeuing from the tail behaves the same as `sojourn/4'.
%% @see sojourn/4
-spec sojourn_r(Time, OutTime, SojournTime, V) -> {Result, Drops, NV} when
      Time :: integer(),
      OutTime :: integer(),
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn_r(Time, OutTime, SojournTime, V) ->
    case handle_sojourn(handle_sojourn_r, Time, OutTime, SojournTime, V) of
        badarg ->
            error(badarg, [Time, OutTime, SojournTime, V]);
        Result ->
            Result
    end.

%% @equiv dropped(time(V), time(V), V)
-spec dropped(V) -> {Result, Drops, NV} when
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped(#svalve{squeue=S} = V) ->
    Time = squeue:time(S),
    dropped(Time, Time, V).

%% @equiv dropped(Time, Time, V)
-spec dropped(Time, V) -> {Result, Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped(Time, V) ->
    dropped(Time, Time, V).

%% @doc Advance time of the queue, `V', to `Time' and signal the dropping of an
%% item, at time `DropTime', to trigger a dequeue from the head of the queue,
%% `V'. Returns the same values as `sojourn/4'.
%%
%% If `DropTime' is less than the last time a sojourn time or drop was signalled
%% to the feedback loop, the drop is signalled with the same time as the last
%% signal. This time may be before the current time of the queue.
%%
%% This function raises the error `badarg' if `DropTime' is greater than `Time'.
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
%% @see sojourn/4
-spec dropped(Time, DropTime, V) -> {Result, Drops, NV} when
      Time :: integer(),
      DropTime :: integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped(Time, DropTime, V) ->
    case handle_dropped(handle_dropped, Time, DropTime, V) of
        badarg ->
            error(badarg, [Time, DropTime, V]);
        Result ->
            Result
    end.

%% @equiv dropped_r(time(V), time(V), V)
-spec dropped_r(V) -> {Result, Drops, NV} when
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped_r(#svalve{squeue=S} = V) ->
    Time = squeue:time(S),
    dropped_r(Time, Time, V).

%% @equiv dropped_r(Time, Time, V)
-spec dropped_r(Time, V) -> {Result, Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped_r(Time, V) ->
    dropped_r(Time, Time, V).

%% @doc Advance time of the queue, `V', to `Time' and signal the dropping of an
%% item, at time `DropTime', to trigger a dequeue from the tail of the queue,
%% `V'.
%%
%% Except for dequeuing from the tail behaves the same as `dropped/3'.
%% @see dropped/3
-spec dropped_r(Time, DropTime, V) -> {Result, Drops, NV} when
      Time :: integer(),
      DropTime :: integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped_r(Time, DropTime, V) ->
    case handle_dropped(handle_dropped_r, Time, DropTime, V) of
        badarg ->
            error(badarg, [Time, DropTime, V]);
        Result ->
            Result
    end.

%% Internal

handle_sojourn(Fun, STime, DropTime, SojournTime,
               #svalve{status=open, module=Module, time=PrevTime, squeue=S,
                       state=State} = V)
  when is_integer(DropTime) andalso STime >= DropTime andalso
       is_integer(SojournTime) andalso SojournTime >= 0 ->
    NS = squeue:time(STime, S),
    NTime = max(PrevTime, DropTime),
    {Result, Drops, NS2, NState} = Module:Fun(NTime, SojournTime, NS, State),
    {Result, Drops, V#svalve{time=NTime, squeue=NS2, state=NState}};
handle_sojourn(_, STime, DropTime, SojournTime,
               #svalve{status=closed, module=Module, time=PrevTime, squeue=S,
                       state=State} = V)
  when is_integer(DropTime) andalso STime >= DropTime andalso
       is_integer(SojournTime) andalso SojournTime >= 0 ->
    NS = squeue:time(STime, S),
    NTime = max(PrevTime, DropTime),
    {closed, Drops, NS2, NState} = Module:handle_sojourn_closed(NTime,
                                                                SojournTime, NS,
                                                                State),
    {closed, Drops, V#svalve{time=NTime, squeue=NS2, state=NState}};
handle_sojourn(_, _, _, _, #svalve{}) ->
    badarg.

handle_dropped(Fun, STime, DropTime,
               #svalve{status=open, module=Module, time=PrevTime, squeue=S,
                       state=State} = V)
  when is_integer(DropTime) andalso STime >= DropTime ->
    NS = squeue:time(STime, S),
    NTime = max(PrevTime, DropTime),
    {Result, Drops, NS2, NState} = Module:Fun(NTime, NS, State),
    {Result, Drops, V#svalve{time=NTime, squeue=NS2, state=NState}};
handle_dropped(_, STime, DropTime,
               #svalve{status=closed, module=Module, time=PrevTime, squeue=S,
                       state=State} = V)
  when is_integer(DropTime) andalso STime >= DropTime ->
    NS = squeue:time(STime, S),
    NTime = max(PrevTime, DropTime),
    {closed, Drops, NS2, NState} = Module:handle_dropped_closed(NTime, NS,
                                                                State),
    {closed, Drops, V#svalve{time=NTime, squeue=NS2, state=NState}};
handle_dropped(_, _, _, #svalve{}) ->
    badarg.
