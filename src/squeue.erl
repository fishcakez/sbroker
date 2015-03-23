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
%% This module provides sojourn-time based active queue management with a
%% similar API to OTP's `queue' module.
%%
%% A subset of the `squeue' API is a subset of the `queue' module's API with one
%% exception: when `{value, Item}' is returned by `queue', `squeue' returns
%% `{SojournTime, Item}', where `SojournTime' (`non_neg_integer()') is the
%% sojourn time of the item (or length of time an item waited in the queue).
%%
%% `squeue' also provides an optional first argument to some functions in common
%% with `queue': `Time'. This argument is of type `non_neg_integer()' and sets
%% the current time of the queue, if `Time' is greater than (or equal) to
%% the queue's previous time. When the `Time' argument is included items
%% may be dropped by the queue's management algorithm. The dropped items
%% will be included in the return value when the queue itself is also
%% returned. The dropped items are a list of the form: `[{SojournTime, Item}]',
%% which is ordered with the item with the greatest `SojournTime' (i.e. the
%% oldest) at the head.
%%
%% `squeue' includes 3 queue management algorithms: `squeue_naive',
%% `squeue_timeout', `squeue_codel'.
-module(squeue).

%% Original API

-export([new/0]).
-export([new/1]).
-export([new/2]).
-export([new/3]).
-export([is_queue/1]).
-export([len/1]).
-export([in/2]).
-export([in/3]).
-export([out/1]).
-export([out/2]).
-export([out_r/1]).
-export([out_r/2]).
-export([to_list/1]).
-export([join/2]).
-export([filter/2]).
-export([filter/3]).

%% Additional API

-export([time/1]).
-export([timeout/2]).

%% Test API

-export([from_start_list/4]).

-ifdef(LEGACY_TYPES).
-record(squeue, {module :: module(),
                 state :: any(),
                 time = 0 :: non_neg_integer(),
                 queue = queue:new() :: queue()}).
-type squeue() :: squeue(any()).
-opaque squeue(_Item) :: #squeue{queue :: queue()}.

-export_type([squeue/0]).
-export_type([squeue/1]).

-callback init(Args :: any()) -> State :: any().
-callback handle_timeout(Time :: non_neg_integer(), Q :: queue(),
                      State :: any()) ->
    {Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
     NQ :: queue(), NState :: any()}.
-callback handle_out(Time :: non_neg_integer(), Q :: queue(),
                      State :: any()) ->
    {Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
     NQ :: queue(), NState :: any()}.
-callback handle_join(Time :: non_neg_integer(), Q :: queue(),
                      State :: any()) ->
    {Drops :: [], NQ :: queue(), NState :: any()}.
-else.
-record(squeue, {module :: module(),
                 state :: any(),
                 time = 0 :: non_neg_integer(),
                 queue = queue:new() :: queue:queue({non_neg_integer(), _})}).
-type squeue() :: squeue(_).
-opaque squeue(Item) ::
    #squeue{queue :: queue:queue({non_neg_integer(), Item})}.

-export_type([squeue/0]).
-export_type([squeue/1]).

-callback init(Args :: any()) -> State :: any().
-callback handle_timeout(Time :: non_neg_integer(), Q :: queue:queue(Item),
                      State :: any()) ->
    {Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
     NQ :: queue:queue(Item), NState :: any()}.
-callback handle_out(Time :: non_neg_integer(), Q :: queue:queue(Item),
                      State :: any()) ->
    {Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
     NQ :: queue:queue(Item), NState :: any()}.
-callback handle_join(Time :: non_neg_integer(), Q :: queue:queue(Item),
                      State :: any()) ->
    {Drops :: [], NQ :: queue:queue(Item), NState :: any()}.
-endif.

%% Original API

%% @doc Returns an empty queue, `S', with the `squeue_naive' management
%% algorithm, and time of `0'.
-spec new() -> S when
      S :: squeue().
new() ->
    new(squeue_naive, undefined).

%% @doc Returns an empty queue, `S', with the `squeue_naive' management
%% algorithm, and time of `Time'.
-spec new(Time) -> S when
      Time :: non_neg_integer(),
      S :: squeue().
new(Time) ->
    new(Time, squeue_naive, undefined).

%% @doc Returns an empty queue, `S', with the `Module' management algorithm
%% started with arguments `Args' and time of `0'.
-spec new(Module, Args) -> S when
      Module :: module(),
      Args :: any(),
      S :: squeue().
new(Module, Args) ->
    #squeue{module=Module, state=Module:init(Args)}.

%% @doc Returns an empty queue, `S', with the `Module' management algorithm
%% started with arguments `Args' and time of `Time'.
-spec new(Time, Module, Args) -> S when
      Time :: non_neg_integer(),
      Module :: module(),
      Args :: any(),
      S :: squeue().
new(Time, Module, Args) when is_integer(Time) andalso Time >= 0 ->
    State = Module:init(Args),
    #squeue{module=Module, time=Time, state=State}.

%% @doc Tests if a term, `Term', is an `squeue' queue, returns `true' if is,
%% otherwise `false'.
-spec is_queue(Term) -> Bool when
      Term :: any(),
      Bool :: boolean().
is_queue(#squeue{}) ->
    true;
is_queue(_Other) ->
    false.

%% @doc Returns the length of the queue, `S'.
-spec len(S) -> Len when
      S :: squeue(),
      Len :: non_neg_integer().
len(#squeue{queue=Q}) ->
    queue:len(Q).

%% @doc Drop items, `Drops', from the queue, `S', and then inserts the item,
%% `Item', at the tail of queue, `S'. Returns `{Drops, NS}', where `Drops' is
%% the list of dropped items and their sojourn times and `NS' is the resulting
%% queue without `Drops' and with `Item'.
-spec in(Item, S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
in(Item, #squeue{module=Module, state=State, time=Time, queue=Q} = S) ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    NS = S#squeue{queue=queue:in({Time, Item}, NQ), state=NState},
    {sojourn_drops(Time, Drops), NS}.

%% @doc Advances the queue, `S', to time `Time' and drops items, `Drops',
%% then inserts the item, `Item', at the tail of queue, `S'. Returns a tuple
%% containing the dropped items and their sojourn times, `Drops', and
%% resulting queue, `NS'.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec in(Time, Item, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
in(Time, Item, #squeue{module=Module, state=State, time=PrevTime, queue=Q} = S)
  when is_integer(Time) andalso Time >= PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    NS = S#squeue{time=Time, queue=queue:in({Time, Item}, NQ), state=NState},
    {sojourn_drops(Time, Drops), NS};
in(Time, Item, S) when is_integer(Time) andalso Time >= 0 ->
    in(Item, S).

%% @doc Drops items, `Drops', from the queue, `S', and then removes the item,
%% `Item', from the head of the remaining queue. Returns
%% `{{SojournTime, Item}, Drops, NS}', where `SojournTime' is the time length of
%% time `Item' spent in the queue, `Drops' is the list of dropped items and
%% their sojourn times and `NS' is the resulting queue without `Drops' and
%% `Item'. If `S' is empty after dropping items `{empty, Drops, S}' is returned.
%%
%% This function is different from `queue:out/1', as the sojourn time
%% is included in the result in the place of the atom `value' and the return
%% value is a 3-tuple with the drop items, `Drops', instead of a 2-tuple.
-spec out(S) -> {Result, Drops, NS} when
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out(#squeue{module=Module, time=Time, queue=Q, state=State} = S) ->
    {Drops, NQ, NState} = Module:handle_out(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    {Result, NQ2} = queue:out(NQ),
    NS = S#squeue{time=Time, queue=NQ2, state=NState},
    case Result of
        empty ->
            {empty, Drops2, NS};
        {value, Item} ->
            {sojourn_time(Time, Item), Drops2, NS}
    end.

%% @doc Advances the queue, `S', to time `Time' and drops items, `Drops', then
%% removes the item, `Item', from the head of queue, `S'. Returns
%% `{{SojournTime, Item}, Drops, NS}', where `SojournTime' is the time length of
%% time `Item' spent in the queue, `Drops' is the list of dropped items and
%% their sojourn times, and `NS' is the resulting queue without the removed and
%% dropped items, If the queue is empty after dropping items
%% `{empty, Drops, NS}' is returned.
%%
%% This function is slightly different from `queue:out/1', as the sojourn time
%% is included in the result in the place of the atom `value' and items can be
%% dropped.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec out(Time, S) -> {Result, Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time >= PrevTime ->
    {Drops, NQ, NState} = Module:handle_out(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    {Result, NQ2} = queue:out(NQ),
    NS = S#squeue{time=Time, queue=NQ2, state=NState},
    case Result of
        empty ->
            {empty, Drops2, NS};
        {value, Item} ->
            {sojourn_time(Time, Item), Drops2, NS}
    end;
out(Time, S) when is_integer(Time) andalso Time >= 0 ->
    out(S).

%% @doc Drops items, `Drops', from the queue, `S', and then removes the item,
%% `Item', from the tail of the remaining queue. Returns
%% `{{SojournTime, Item}, Drops, NS}', where `SojournTime' is the time length of
%% time `Item' spent in the queue, `Drops' is the list of dropped items and
%% their sojourn times and `NS' is the resulting queue without `Drops' and
%% `Item'. If `S' is empty after dropping items `{empty, Drops, S}' is returned.
%%
%% This function is different from `queue:out_r/1', as the sojourn time
%% is included in the result in the place of the atom `value' and the return
%% value is a 3-tuple with the drop items, `Drops', instead of a 2-tuple.
-spec out_r(S) -> {Result, Drops, NS} when
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out_r(#squeue{module=Module, time=Time, queue=Q, state=State} = S) ->
    {Drops, NQ, NState} = Module:handle_out_r(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    {Result, NQ2} = queue:out_r(NQ),
    NS = S#squeue{time=Time, queue=NQ2, state=NState},
    case Result of
        empty ->
            {empty, Drops2, NS};
        {value, Item} ->
            {sojourn_time(Time, Item), Drops2, NS}
    end.

%% @doc Advances the queue, `S', to time `Time' and drops items, `Drops', then
%% removes the item, `Item', from the tail of queue, `S'. Returns
%% `{{SojournTime, Item}, Drops, NS}', where `SojournTime' is the time length of
%% time `Item' spent in the queue, `Drops' is the list of dropped items and
%% their sojourn times and `NS' is the resulting queue without the removed and
%% dropped items, If the queue is empty after dropping items
%% `{empty, Drops, NS}' is returned.
%%
%% This function is slightly different from `queue:out_r/1', as the sojourn time
%% is included in the result in the place of the atom `value' and items can be
%% dropped.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec out_r(Time, S) -> {Result, Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time >= PrevTime ->
    {Drops, NQ, NState} = Module:handle_out_r(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    {Result, NQ2} = queue:out_r(NQ),
    NS = S#squeue{time=Time, queue=NQ2, state=NState},
    case Result of
        empty ->
            {empty, Drops2, NS};
        {value, Item} ->
            {sojourn_time(Time, Item), Drops2, NS}
    end;
out_r(Time, S) when is_integer(Time) andalso Time >= 0 ->
    out_r(S).

%% @doc Returns a list of items, `List', in the queue, `S'.
%%
%% The order of items in `List' matches their order in the queue, `S', so that
%% the item at the head of the queue is at the head of the list.
-spec to_list(S) -> List when
      S :: squeue(Item),
      List :: [Item].
to_list(#squeue{queue=Q}) ->
    [Item || {_Start, Item} <- queue:to_list(Q)].

%% @doc Joins two queues, `S1' and `S2', into one queue, `NS', with the items in
%% `S1' at the head and the items in `S2' at the tail.
%%
%% This function raises the error `badarg' if any item in queue `S1' was added
%% after any item in queue `S2'.
%%
%% This function raises the error `badarg' if the current time of the queues,
%% `S1' and `S2', are not the same.
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
            error(badarg, [S1, S2]);
        _ ->
            {[], NQ1, NState1} = Module1:handle_join(Time, Q1, State1),
            NQ = queue:join(NQ1, Q2),
            %% handle_join/1 is required to notify the queue manager that the
            %% max sojourn time of the queue may have increased (though only if
            %% the head queue was empty).
            S1#squeue{queue=NQ, state=NState1}
    end;
join(#squeue{} = S1, #squeue{} = S2) ->
    error(badarg, [S1, S2]).

%% @doc Applys a fun, `Filter', to all items in the queue, `S', and returns the
%% resulting queue, `NS'.
%%
%% If `Filter(Item)' returns `true', the item appears in the new queue.
%%
%% If `Filter(Item)' returns `false', the item does not appear in the new
%% queue.
%%
%% If `Filter(Item)' returns a list of items, these items appear in the new
%% queue with all items having the start time of the origin item, `Item'.
-spec filter(Filter, S) -> {Drops, NS} when
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
filter(Filter, #squeue{module=Module, time=Time, queue=Q, state=State} = S) ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    NQ2 = queue:filter(make_filter(Filter), NQ),
    {Drops2, S#squeue{queue=NQ2, state=NState}}.

%% @doc Advances the queue, `S', to time `Time'  and drops items, then applys a
%% fun, `Filter', to all remaining items in the queue. Returns a tuple
%% containing the dropped items and their sojourn times, `Drops', and the new
%% queue, `NS'.
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
-spec filter(Time, Filter, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
filter(Time, Filter,
       #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time >= PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    NQ2 = queue:filter(make_filter(Filter), NQ),
    NS = S#squeue{time=Time, queue=NQ2, state=NState},
    {Drops2, NS};
filter(Time, Filter, S) when is_integer(Time) andalso Time >= 0 ->
    filter(Filter, S).

%% Additional API

%% @doc Returns the current time , `Time', of the queue, `S'.
-spec time(S) -> Time when
      S :: squeue(),
      Time :: non_neg_integer().
time(#squeue{time=Time}) ->
    Time.

%% @doc Advances the queue, `S', to time `Time' and drops item. Returns a tuple
%% containing the dropped items and their sojourn times, `Drops', and resulting
%% queue, `NS'.
%%
%% If `Time' is less than the current time of the queue time, the current time
%% is used instead.
-spec timeout(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
timeout(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time >= PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {sojourn_drops(Time, Drops), S#squeue{time=Time, queue=NQ, state=NState}};
timeout(Time, #squeue{time=PrevTime} = S)
  when is_integer(Time) andalso Time >= 0 ->
    timeout(PrevTime, S).

%% Test API

%% @hidden
from_start_list(Time, List, Module, Args) ->
    S = new(Time, Module, Args),
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
