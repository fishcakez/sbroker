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
%% and `squeue/2'. `sojourn/2' and `sojourn/3' signal the feedback loop to
%% dequeue from the head of the queue. `dropped/1' and `dropped/2' signal a drop
%% and can also result in a dequeue from the head. Alternatively `sojourn_r/2',
%% `sojourn_r/3', `dropped_r/1' and `dropped_r/2' are used to dequeue from the
%% tail.
%%
%% All other functions are equivalent to `squeue'.
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
-export([timeout/2]).

%% Additional API

-export([open/1]).
-export([close/1]).
-export([squeue/1]).
-export([squeue/2]).
-export([sojourn/2]).
-export([sojourn/3]).
-export([sojourn_r/2]).
-export([sojourn_r/3]).
-export([dropped/1]).
-export([dropped/2]).
-export([dropped_r/1]).
-export([dropped_r/2]).


%% types

-record(svalve, {module :: module(),
                 state :: any(),
                 status = open :: open | closed,
                 time = 0 :: non_neg_integer(),
                 squeue = squeue:new() :: squeue:squeue(any())}).

-type svalve() :: svalve(any()).
-opaque svalve(Item) :: #svalve{squeue :: squeue:squeue(Item)}.

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

%% @doc Returns an empty queue, `V', with the `svalve_naive' feedback loop,
%% using an `squeue' managed by `squeue_naive' and time of `0'.
-spec new() -> V when
      V :: svalve().
new() ->
    new(svalve_naive, undefined).

%% @doc Returns an empty queue, `V', with the `svalve_naive' feedback loop,
%% using an `squeue' managed by `squeue_naive' and time of `Time'.
-spec new(Time) -> V when
      Time :: integer(),
      V :: svalve().
new(Time) ->
    new(Time, svalve_naive, undefined).

%% @doc Returns an empty queue, `V', with the `Module' feedback loop started
%% with arguments `Args', using an `squeue' managed by `squeue_naive' and time
%% of `0'.
-spec new(Module, Args) -> V when
      Module :: module(),
      Args :: any(),
      V :: svalve().
new(Module, Args) ->
    new(0, Module, Args).

%% @doc Returns an empty queue, `V', with the `Module' feedback loop started
%% with arguments `Args', using an `squeue' managed by `squeue_naive' and time
%% of `Time'.
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
-spec is_queue(Term) -> Bool when
      Term :: any(),
      Bool :: boolean().
is_queue(#svalve{}) ->
    true;
is_queue(_Other) ->
    false.

%% @doc Returns the length of the queue, `V'.
-spec len(V) -> Len when
      V :: svalve(),
      Len :: non_neg_integer().
len(#svalve{squeue=S}) ->
    squeue:len(S).

%% @doc Drop items, `Drops', from the queue, `V', and then inserts the item,
%% `Item', at the tail of queue, `V'. Returns `{Drops, NV}', where `Drops' is
%% the list of dropped items and their sojourn times and `NV' is the resulting
%% queue without `Drops' and with `Item'.
-spec in(Item, V) -> {Drops, NV} when
      V :: svalve(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
in(Item, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:in(Item, S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and drops item, then inserts
%% the item, `Item', at the tail of queue, `V'. Returns a tuple containing the
%% dropped items and their sojourn times, `Drops', and resulting queue, `NV'.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec in(Time, Item, V) -> {Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
in(Time, Item, #svalve{time=PrevTime, squeue=S} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NS} = squeue:in(Time, Item, S),
    {Drops, V#svalve{time=Time, squeue=NS}};
in(Time, Item, #svalve{squeue=S} = V) when is_integer(Time) ->
    {Drops, NS} = squeue:in(Time, Item, S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and drops item, then inserts
%% the item, `Item', with time `InTime' at the tail of queue, `V'. Returns a
%% tuple containing the dropped items and their sojourn times, `Drops', and
%% resulting queue, `NV'.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
%%
%% If `InTime' is less than the last time an item was inserted at the tail of
%% the queue, the item is inserted with the same time as the last item. This
%% time may be before the current time of the queue.
%%
%% This function raises the error `badarg' if `InTime' is greater than `Time'.
-spec in(Time, InTime, Item, V) -> {Drops, NV} when
      Time :: integer(),
      InTime :: integer(),
      V :: svalve(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
in(Time, InTime, Item, #svalve{time=PrevTime, squeue=S} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NS} = squeue:in(Time, InTime, Item, S),
    {Drops, V#svalve{time=Time, squeue=NS}};
in(Time, InTime, Item, #svalve{squeue=S} = V) when is_integer(Time) ->
    {Drops, NS} = squeue:in(Time, InTime, Item, S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Drops items, `Drops', from the queue, `V', and then removes the item,
%% `Item', from the head of the remaining queue. Returns
%% `{{SojournTime, Item}, Drops, NV}', where `SojournTime' is the time length of
%% time `Item' spent in the queue, `Drops' is the list of dropped items and
%% their sojourn times and `NV' is the resulting queue without `Drops' and
%% `Item'. If `V' is empty after dropping items `{empty, Drops, V}' is returned.
%%
%% This function is different from `queue:out/1', as the sojourn time
%% is included in the result in the place of the atom `value' and the return
%% value is a 3-tuple with the drop items, `Drops', instead of a 2-tuple.
-spec out(V) -> {Result, Drops, NV} when
      V :: svalve(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
out(#svalve{squeue=S} = V) ->
    {Result, Drops, NS} = squeue:out(S),
    {Result, Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and drops items, `Drops', then
%% removes the item, `Item', from the head of queue, `V'. Returns
%% `{{SojournTime, Item}, Drops, NV}', where `SojournTime' is the time length of
%% time `Item' spent in the queue, `Drops' is the list of dropped items and
%% their sojourn times, and `NV' is the resulting queue without the removed and
%% dropped items, If the queue is empty after dropping items
%% `{empty, Drops, NV}' is returned.
%%
%% This function is slightly different from `queue:out/1', as the sojourn time
%% is included in the result in the place of the atom `value' and items can be
%% dropped.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec out(Time, V) -> {Result, Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
out(Time, #svalve{time=PrevTime, squeue=S} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {Result, Drops, NS} = squeue:out(Time, S),
    {Result, Drops, V#svalve{time=Time, squeue=NS}};
out(Time, V) when is_integer(Time) ->
    out(V).

%% @doc Drops items, `Drops', from the queue, `V', and then removes the item,
%% `Item', from the tail of the remaining queue. Returns
%% `{{SojournTime, Item}, Drops, NV}', where `SojournTime' is the time length of
%% time `Item' spent in the queue, `Drops' is the list of dropped items and
%% their sojourn times and `NV' is the resulting queue without `Drops' and
%% `Item'. If `V' is empty after dropping items `{empty, Drops, V}' is returned.
%%
%% This function is different from `queue:out_r/1', as the sojourn time
%% is included in the result in the place of the atom `value' and the return
%% value is a 3-tuple with the drop items, `Drops', instead of a 2-tuple.
-spec out_r(V) -> {Result, Drops, NV} when
      V :: svalve(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
out_r(#svalve{squeue=S} = V) ->
    {Result, Drops, NS} = squeue:out_r(S),
    {Result, Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and drops items, `Drops', then
%% removes the item, `Item', from the tail of queue, `V'. Returns
%% `{{SojournTime, Item}, Drops, NV}', where `SojournTime' is the time length of
%% time `Item' spent in the queue, `Drops' is the list of dropped items and
%% their sojourn times and `NV' is the resulting queue without the removed and
%% dropped items, If the queue is empty after dropping items
%% `{empty, Drops, NV}' is returned.
%%
%% This function is slightly different from `queue:out_r/1', as the sojourn time
%% is included in the result in the place of the atom `value' and items can be
%% dropped.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec out_r(Time, V) -> {Result, Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
out_r(Time, #svalve{time=PrevTime, squeue=S} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {Result, Drops, NS} = squeue:out_r(Time, S),
    {Result, Drops, V#svalve{time=Time, squeue=NS}};
out_r(Time, V) when is_integer(Time) ->
    out_r(V).

%% @doc Drops items, `Drops', from the queue, `V'. Returns `{Drops, NV}',
%% where `Drops' is the list of dropped items and their sojourn times and `NV'
%% is the resulting queue without `Drops'. If `V' is empty raises an `empty'
%% error.
%%
%% If the active queue management callback module does not drop any items, the
%% item at the head of the queue is dropped.
%%
%% This function is different from `queue:drop/1', as the return value is a
%% 2-tuple with the dropped items, `Drops' and the new queue , `NV', instead of
%% just the new queue.
-spec drop(V) -> {Drops, NV} when
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
drop(#svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:drop(S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and drops items, `Drops', from
%% the queue. Returns `{Drops, NV}', where `Drops' is the list of dropped items
%% and their sojourn times, and `NV' is the resulting queue without the dropped
%% items. If `V' is empty rauses an `empty' error.
%%
%% If the active queue management callback module does not drop any items, the
%% item at the head of the queue is dropped.
%%
%% This function is different from `queue:drop/1', as the return value is a
%% 2-tuple with the dropped items, `Drops' and the new queue , `NV', instead of
%% just the new queue.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec drop(Time, V) -> {Drops, NV} when
      Time :: non_neg_integer(),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
drop(Time, #svalve{time=PrevTime, squeue=S} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NS} = squeue:drop(Time, S),
    {Drops, V#svalve{time=Time, squeue=NS}};
drop(Time, V) when is_integer(Time) ->
    drop(V).

%% @doc Drops items, `Drops', from the queue, `V'. Returns `{Drops, NV}',
%% where `Drops' is the list of dropped items and their sojourn times and `NV'
%% is the resulting queue without `Drops'. If `V' is empty raises an `empty'
%% error.
%%
%% If the active queue management callback module does not drop any items, the
%% item at the tail of the queue is dropped.
%%
%% This function is different from `queue:drop/1', as the return value is a
%% 2-tuple with the dropped items, `Drops' and the new queue , `NV', instead of
%% just the new queue. Also the dropped item or items may not be from the tail
%% of the queue.
-spec drop_r(V) -> {Drops, NV} when
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
drop_r(#svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:drop_r(S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time' and drops items, `Drops', from
%% the queue. Returns `{Drops, NV}', where `Drops' is the list of dropped items
%% and their sojourn times, and `NV' is the resulting queue without the dropped
%% items. If `V' is empty rauses an `empty' error.
%%
%% If the active queue management callback module does not drop any items, the
%% item at the tail the queue is dropped.
%%
%% This function is different from `queue:drop_r/1', as the return value is a
%% 2-tuple with the dropped items, `Drops' and the new queue , `NV', instead of
%% just the new queue. Also the dropped item or items may not be from the tail
%% of the queue.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec drop_r(Time, V) -> {Drops, NV} when
      Time :: non_neg_integer(),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
drop_r(Time, #svalve{time=PrevTime, squeue=S} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NS} = squeue:drop_r(Time, S),
    {Drops, V#svalve{time=Time, squeue=NS}};
drop_r(Time, V) when is_integer(Time) ->
    drop_r(V).

%% @doc Returns a list of items, `List', in the queue, `V'.
%%
%% The order of items in `List' matches their order in the queue, `V', so that
%% the item at the head of the queue is at the head of the list.
-spec to_list(V) -> List when
      V :: svalve(Item),
      List :: [Item].
to_list(#svalve{squeue=S}) ->
    squeue:to_list(S).

%% @doc Joins two queues, `V1' and `V2', into one queue, `VS', with the items in
%% `V1' at the head and the items in `V2' at the tail.
%%
%% This function raises the error `badarg' if any item in queue `V1' was added
%% after any item in queue `V2'.
%%
%% This function raises the error `badarg' if the current time of the queues,
%% `V1' and `V2', are not the same.
-spec join(V1, V2) -> NV when
      V1 :: svalve(Item),
      V2 :: svalve(Item),
      NV :: svalve(Item).
%% To merge two queues they must have the same Time.
join(#svalve{time=Time, squeue=S1} = V1, #svalve{time=Time, squeue=S2}) ->
    NS = squeue:join(S1, S2),
    V1#svalve{squeue=NS};
join(#svalve{} = V1, #svalve{} = V2) ->
    error(badarg, [V1, V2]).

%% @doc Applys a fun, `Filter', to all items in the queue, `V', and returns the
%% resulting queue, `NV'.
%%
%% If `Filter(Item)' returns `true', the item appears in the new queue.
%%
%% If `Filter(Item)' returns `false', the item does not appear in the new
%% queue.
%%
%% If `Filter(Item)' returns a list of items, these items appear in the new
%% queue with all items having the start time of the origin item, `Item'.
-spec filter(Filter, V) -> {Drops, NV} when
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
filter(Filter, #svalve{squeue=S} = V) ->
    {Drops, NS} = squeue:filter(Filter, S),
    {Drops, V#svalve{squeue=NS}}.

%% @doc Advances the queue, `V', to time `Time'  and drops items, then applys a
%% fun, `Filter', to all remaining items in the queue. Returns a tuple
%% containing the dropped items and their sojourn times, `Drops', and the new
%% queue, `NV'.
%%
%% If `Filter(Item)' returns `true', the item appears in the new queue.
%%
%% If `Filter(Item)' returns `false', the item does not appear in the new
%% queue.
%%
%% If `Filter(Item)' returns a list of items, these items appear in the new
%% queue with all items having the start time of the origin item, `Item'.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.

-spec filter(Time, Filter, V) -> {Drops, NV} when
      Time :: integer(),
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
filter(Time, Filter, #svalve{time=PrevTime, squeue=S} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NS} = squeue:filter(Time, Filter, S),
    {Drops, V#svalve{time=Time, squeue=NS}};
filter(Time, Item, V) when is_integer(Time) ->
    filter(Item, V).

%% @doc Returns the current time , `Time', of the queue, `V'.
-spec time(V) -> Time when
      V :: svalve(),
      Time :: integer().
time(#svalve{time=Time}) ->
    Time.

%% @doc Advances the queue, `V', to time `Time' and drops item. Returns a tuple
%% containing the dropped items and their sojourn times, `Drops', and resulting
%% queue, `NV'.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec timeout(Time, V) -> {Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve().
timeout(Time, #svalve{time=PrevTime, squeue=S} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NS} = squeue:timeout(Time, S),
    {Drops, V#svalve{time=Time, squeue=NS}};
timeout(Time, #svalve{time=PrevTime, squeue=S} = V)
  when is_integer(Time) ->
    {Drops, NS} = squeue:timeout(PrevTime, S),
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
squeue(S, #svalve{time=Time} = V) ->
    case squeue:time(S) of
        Time ->
            V#svalve{squeue=S};
        _ ->
            error(badarg, [S, V])
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

%% @doc Signal the sojourn time, `SojournTime', of another queue to trigger a
%% dequeue from the head of the queue, `V'.
-spec sojourn(SojournTime, V) -> {Result, Drops, NV} when
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn(SojournTime, V) ->
    handle_sojourn(handle_sojourn, SojournTime, V).

%% @doc Advance time of the queue, `V', to `Time' and signal the sojourn time,
%% `SojournTime', of another queue to trigger a dequeue from the head of the
%% queue, `V'.
-spec sojourn(Time, SojournTime, V) -> {Result, Drops, NV} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn(Time, SojournTime, V) ->
    handle_sojourn(handle_sojourn, Time, SojournTime, V).

%% @doc Signal the sojourn time, `SojournTime', of another queue to trigger a
%% dequeue from the tail of the queue, `V'.
-spec sojourn_r(SojournTime, V) -> {Result, Drops, NV} when
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn_r(SojournTime, V) ->
    handle_sojourn(handle_sojourn_r, SojournTime, V).

%% @doc Advance time of the queue, `V', to `Time' and signal the sojourn time,
%% `SojournTime', of another queue to trigger a dequeue from the tail of the
%% queue, `V'.
-spec sojourn_r(Time, SojournTime, V) -> {Result, Drops, NV} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
sojourn_r(Time, SojournTime, V) ->
    handle_sojourn(handle_sojourn_r, Time, SojournTime, V).

%% @doc Signal the dropping of an item to trigger a dequeue from the head of the
%% queue, `V'.
-spec dropped(V) -> {Result, Drops, NV} when
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped(V) ->
    handle_dropped(handle_dropped, V).

%% @doc Advance time of the queue, `V', to `Time' and signal the dropping of an
%% item to trigger a dequeue from the head of the queue, `V'.
-spec dropped(Time, V) -> {Result, Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped(Time, V) ->
    handle_dropped(handle_dropped, Time, V).

%% @doc Signal the dropping of an item to trigger a dequeue from the tail of the
%% queue, `V'.
-spec dropped_r(V) -> {Result, Drops, NV} when
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped_r(V) ->
    handle_dropped(handle_dropped_r, V).

%% @doc Advance time of the queue, `V', to `Time' and signal the dropping of an
%% item to trigger a dequeue from the tail of the queue, `V'.
-spec dropped_r(Time, V) -> {Result, Drops, NV} when
      Time :: integer(),
      V :: svalve(Item),
      Result :: empty | closed | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NV :: svalve(Item).
dropped_r(Time, V) ->
    handle_dropped(handle_dropped_r, Time, V).

%% Internal

handle_sojourn(Fun, SojournTime,
               #svalve{status=open, module=Module, time=Time, squeue=S,
                       state=State} = V)
  when is_integer(SojournTime) andalso SojournTime >= 0 ->
    {Result, Drops, NS, NState} = Module:Fun(Time, SojournTime, S, State),
    {Result, Drops, V#svalve{squeue=NS, state=NState}};
handle_sojourn(_, SojournTime,
               #svalve{status=closed, module=Module, time=Time, squeue=S,
                       state=State} = V)
  when is_integer(SojournTime) andalso SojournTime >= 0 ->
    {closed, Drops, NS, NState} = Module:handle_sojourn_closed(Time,
                                                               SojournTime, S,
                                                               State),
    {closed, Drops, V#svalve{squeue=NS, state=NState}}.

handle_sojourn(Fun, Time, SojournTime,
               #svalve{status=open, module=Module, time=PrevTime, squeue=S,
                       state=State} = V)
  when is_integer(Time) andalso Time > PrevTime andalso
       is_integer(SojournTime) andalso SojournTime >= 0 ->
    {Result, Drops, NS, NState} = Module:Fun(Time, SojournTime, S, State),
    {Result, Drops, V#svalve{time=Time, squeue=NS, state=NState}};
handle_sojourn(_, Time, SojournTime,
               #svalve{status=closed, module=Module, time=PrevTime, squeue=S,
                       state=State} = V)
  when is_integer(Time) andalso Time > PrevTime andalso
       is_integer(SojournTime) andalso SojournTime >= 0 ->
    {closed, Drops, NS, NState} = Module:handle_sojourn_closed(Time,
                                                               SojournTime, S,
                                                               State),
    {closed, Drops, V#svalve{time=Time, squeue=NS, state=NState}};
handle_sojourn(Fun, Time, SojournTime, V) when is_integer(Time) ->
    handle_sojourn(Fun, SojournTime, V).

handle_dropped(Fun,
               #svalve{status=open, module=Module, time=Time, squeue=S,
                       state=State} = V) ->
    {Result, Drops, NS, NState} = Module:Fun(Time, S, State),
    {Result, Drops, V#svalve{squeue=NS, state=NState}};
handle_dropped(_,
               #svalve{status=closed, module=Module, time=Time, squeue=S,
                       state=State} = V) ->
    {closed, Drops, NS, NState} = Module:handle_dropped_closed(Time, S, State),
    {closed, Drops, V#svalve{squeue=NS, state=NState}}.

handle_dropped(Fun, Time,
               #svalve{status=open, module=Module, time=PrevTime, squeue=S,
                       state=State} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {Result, Drops, NS, NState} = Module:Fun(Time, S, State),
    {Result, Drops, V#svalve{time=Time, squeue=NS, state=NState}};
handle_dropped(_, Time,
               #svalve{status=closed, module=Module, time=PrevTime, squeue=S,
                       state=State} = V)
  when is_integer(Time) andalso Time > PrevTime ->
    {closed, Drops, NS, NState} = Module:handle_dropped_closed(Time, S, State),
    {closed, Drops, V#svalve{time=Time, squeue=NS, state=NState}};
handle_dropped(Fun, Time, V) when is_integer(Time) ->
    handle_dropped(Fun, V).
