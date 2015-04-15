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
%% A subset of the `squeue' API is a similar to a subset of the `queue' API.
%% There are two main differences. Firstly when `{value, Item}' would be
%% returned by `queue', `squeue' returns `{SojournTime, Item}', where
%% `SojournTime' (`non_neg_integer()') is the sojourn time of the item (or
%% length of time an item waited in the queue).
%%
%% Secondly that items may be dropped by the queue's management algorithm. The
%% dropped items will be included in the return value when the queue itself is
%% also returned. The dropped items are a list of the form:
%% `[{SojournTime, Item}]',  which is ordered with the item with the greatest
%% `SojournTime' (i.e. the oldest) at the head.
%%
%% `squeue' also provides an optional first argument to some functions in common
%% with `queue': `Time'. This argument is of type `integer()' and sets the
%% current time of the queue, if `Time' is greater than (or equal) to the
%% queue's previous time.
%%
%% `squeue' includes 4 queue management algorithms: `squeue_naive',
%% `squeue_timeout', `squeue_codel' and `squeue_codel_timeout'.
%%
%% A custom queue management algorithm must implement the `squeue' behaviour.
%% The first callback is `init/2':
%% ```
%% -callback init(Time :: integer(), Args :: any()) -> State :: any().
%% '''
%% `Time' is the time of the queue at creation. It is `0' for `squeue:new/2' and
%% for `squeue:new/3' it is the `Time' (first) argument. `Time' can be positive
%% or negative. All other callbacks will receive the current time of the queue
%% as the first argument. It is monotically increasing, so subsequent calls will
%% use the same or a greater time.
%%
%% `Args' is the last argument passed to `new/2' or `new/3'. It can be any term.
%%
%% The queue management callback module is called before any `squeue' function
%% alters an internal queue. Before `squeue:in/2,3' and `squeue:filter/2,3' the
%% callback function is `handle_timeout/3':
%% ```
%% -callback handle_timeout(Time ::integer(), Q :: internal_queue(Item),
%%                          State :: any()) ->
%%     {Drops :: [{InTime :: integer(), Item}], NQ :: internal_queue(Item),
%%      NState :: any()}.
%% '''
%% `Q' is the internal representation of the queue. It is a `queue' with 2-tuple
%% items of the form `{InTime, Item}'. `InTime' is the time the item was
%% inserted into the queue. The insertion time may lag behind the current time
%% of the queue. `Item' is the item itself. The order of items in the queue is
%% such that an item has a `InTime' less than or equal to the `InTime'  of all
%% items behind it in the queue. `NQ' is the queue after dropping any items.
%%
%% `State' is the state of the callback module, and `NState' is the new state
%% after applying the queue management.
%%
%% `Drops' is a list of dropped items. The items must be ordered with item
%% nearest the tail of queue nearest the head of the list. For example:
%% ```
%% {DropQ, NQ} = queue:split(N, Q),
%% Drops = lists:reverse(queue:to_list(DropQ)),
%% {Drops, NQ, NState}.
%% '''
%% `squeue' will set the sojourn times and reverse the list before returning the
%% drops. All items from `Q' must either be in `Drops' or `NQ', but not both.
%%
%% When an item is dequeued from the head of queue with `squeue:out/1,2', the
%% callback function `handle_out/3' is called to dequeue the item:
%% ```
%% -callback handle_out(Time ::integer(), Q :: internal_queue(Item),
%%                     State :: any()) ->
%%    {empty | {InTime :: integer(), Item},
%%     Drops :: [{InTime2 :: integer(), Item}],
%%     NQ :: internal_queue(Item), NState :: any()}.
%% '''
%% `Time', `Q' and `State' represent the current time, the internal queue and
%% the state of the callback module, respectively.
%%
%% The first element of the returned 3-tuple is `empty' if `NQ' is empty.
%% Otherwise it is `{InTime, Item}', the item at the head of the queue after
%% dropping `Drops'. `NQ' is the queue after dequeuing drops and the head of the
%% queue. For example:
%% ```
%% case queue:out(Q) of
%%     {empty, NQ} ->
%%         {empty, Drops, NQ, NState};
%%     {{value, {InTime, Item}}, NQ} ->
%%         {{InTime, Item}, Drops, NQ, NState}
%% end.
%% '''
%%
%% Similarly when an item is dequeued from the tail of the queue with
%% `squeue:out_r/1,2', the callback function `handle_out_r/3':
%% ```
%% -callback handle_out_r(Time ::integer(), Q :: internal_queue(Item),
%%                        State :: any()) ->
%%     {empty | {InTime :: integer(), Item},
%%      Drops :: [{InTime2 :: integer(), Item}],
%%      NQ :: internal_queue(Item), NState :: any()}.
%% '''
%% This callback is the same as `handle_out/3', except the item is dequeued from
%% the tail.
%%
%% Before joining a queue to the tail of the queue with `squeue:join/2', the
%% callback function `handle_join/3' is called:
%% ```
%% -callback handle_join(Time ::integer(), Q :: internal_queue(Item :: any()),
%%                       State :: any()) ->
%%    NState :: any().
%% '''
%% This callback can update the state but not drop items or change the internal
%% queue.
%% @see squeue_naive
%% @see squeue_timeout
%% @see squeue_codel
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

%% Additional API

-export([time/1]).
-export([time/2]).
-export([timeout/1]).
-export([timeout/2]).

%% Test API

-export([from_start_list/5]).

-ifdef(LEGACY_TYPES).
-type internal_queue(_Any) :: queue().

-record(squeue, {module :: module(),
                 state :: any(),
                 time = 0 :: integer(),
                 tail_time = 0 :: integer(),
                 queue = queue:new() :: internal_queue(any())}).

-type squeue() :: squeue(any()).
-type squeue(Item) :: #squeue{queue :: internal_queue(Item)}.
-else.
-type internal_queue(Item) :: queue:queue({integer(), Item}).

-record(squeue, {module :: module(),
                 state :: any(),
                 time = 0 :: integer(),
                 tail_time = 0 :: integer(),
                 queue = queue:new() :: internal_queue(any())}).

-type squeue() :: squeue(any()).
-opaque squeue(Item) :: #squeue{queue :: internal_queue(Item)}.
-endif.

-export_type([internal_queue/1]).
-export_type([squeue/0]).
-export_type([squeue/1]).

-callback init(Time :: integer(), Args :: any()) -> State :: any().
-callback handle_timeout(Time ::integer(), Q :: internal_queue(Item),
                         State :: any()) ->
    {Drops :: [{InTime :: integer(), Item}], NQ :: internal_queue(Item),
     NState :: any()}.
-callback handle_out(Time ::integer(), Q :: internal_queue(Item),
                     State :: any()) ->
    {empty | {InTime :: integer(), Item},
     Drops :: [{InTime2 :: integer(), Item}],
     NQ :: internal_queue(Item), NState :: any()}.
-callback handle_out_r(Time ::integer(), Q :: internal_queue(Item),
                       State :: any()) ->
    {empty | {InTime :: integer(), Item},
     Drops :: [{InTime2 :: integer(), Item}],
     NQ :: internal_queue(Item), NState :: any()}.
-callback handle_join(Time ::integer(), Q :: internal_queue(Item :: any()),
                      State :: any()) ->
    NState :: any().

%% Original API

%% @equiv new(0, squeue_naive, undefined)
-spec new() -> S when
      S :: squeue().
new() ->
    new(squeue_naive, undefined).

%% @equiv new(Time, squeue_naive, undefined)
-spec new(Time) -> S when
      Time :: integer(),
      S :: squeue().
new(Time) ->
    new(Time, squeue_naive, undefined).

%% @equiv new(0, Module, Args)
-spec new(Module, Args) -> S when
      Module :: module(),
      Args :: any(),
      S :: squeue().
new(Module, Args) ->
    new(0, Module, Args).

%% @doc Returns an empty queue, `S', with the `Module' management algorithm
%% started with arguments `Args' and time of `Time'.
-spec new(Time, Module, Args) -> S when
      Time :: integer(),
      Module :: module(),
      Args :: any(),
      S :: squeue().
new(Time, Module, Args) when is_integer(Time) ->
    State = Module:init(Time, Args),
    #squeue{module=Module, time=Time, tail_time=Time, state=State}.

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

%% @equiv in(time(S), time(S), Item, S)
-spec in(Item, S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
in(Item, #squeue{time=Time} = S) ->
    in(Time, Time, Item, S).

%% @equiv in(Time, Time, Item, S)
-spec in(Time, Item, S) -> {Drops, NS} when
      Time :: integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
in(Time, Item, S) ->
    in(Time, Time, Item, S).

%% @doc Advances the queue, `S', to time `Time' and then inserts the item,
%% `Item', with time `InTime' at the tail of queue, `S'. Returns a tuple
%% containing the dropped items and their sojourn times, `Drops', and resulting
%% queue, `NS'.
%%
%% If `InTime' is less than the last time an item was inserted at the tail of
%% the queue, the item is inserted with the same time as the last item. This
%% time may be before the current time of the queue.
%%
%% This function raises the error `badarg' if `InTime' is greater than `Time'.
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
-spec in(Time, InTime, Item, S) -> {Drops, NS} when
      Time :: integer(),
      InTime :: integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
in(Time, InTime, Item,
   #squeue{module=Module, state=State, time=PrevTime, tail_time=TailTime,
           queue=Q} = S)
  when is_integer(Time), Time >= PrevTime, is_integer(InTime), Time >= InTime ->
    NTailTime = max(InTime, TailTime),
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    NS = S#squeue{time=Time, tail_time=NTailTime,
                  queue=queue:in({NTailTime, Item}, NQ), state=NState},
    {sojourn_drops(Time, Drops), NS};
in(Time, InTime, Item, #squeue{} = S)
  when is_integer(Time), is_integer(InTime) ->
    error(badarg, [Time, InTime, Item, S]).

%% @equiv out(time(S), S)
-spec out(S) -> {Result, Drops, NS} when
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out(#squeue{time=Time} = S) ->
    out(Time, S).

%% @doc Advances the queue, `S', to time `Time' and removes the item, `Item',
%% from the head of queue, `S'. Returns `{{SojournTime, Item}, Drops, NS}',
%% where `SojournTime' is the time length of time `Item' spent in the queue,
%% `Drops' is the list of dropped items and their sojourn times, and `NS' is
%% the resulting queue without the removed and dropped items. If the queue is
%% empty after dropping items `{empty, Drops, NS}' is returned.
%%
%% This function is slightly different from `queue:out/1', as the sojourn time
%% is included in the result in the place of the atom `value' and items can be
%% dropped.
%%
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
-spec out(Time, S) -> {Result, Drops, NS} when
      Time :: integer(),
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time), Time >= PrevTime ->
    {Result, Drops, NQ, NState} = Module:handle_out(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    NS = S#squeue{time=Time, queue=NQ, state=NState},
    case Result of
        empty ->
            {empty, Drops2, NS};
        Item ->
            {sojourn_time(Time, Item), Drops2, NS}
    end;
out(Time, #squeue{} = S) when is_integer(Time) ->
    error(badarg, [Time, S]).

%% @equiv out_r(time(S), S)
-spec out_r(S) -> {Result, Drops, NS} when
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out_r(#squeue{module=Module, time=Time, queue=Q, state=State} = S) ->
    {Result, Drops, NQ, NState} = Module:handle_out_r(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    NS = S#squeue{queue=NQ, state=NState},
    case Result of
        empty ->
            {empty, Drops2, NS};
        Item ->
            {sojourn_time(Time, Item), Drops2, NS}
    end.

%% @doc Advances the queue, `S', to time `Time' and removes the item, `Item',
%% from the tail of queue, `S'. Returns `{{SojournTime, Item}, Drops, NS}',
%% where `SojournTime' is the time length of time `Item' spent in the queue,
%% `Drops' is the list of dropped items and their sojourn times and `NS' is the
%% resulting queue without the removed and dropped items. If the queue is empty
%% after dropping items `{empty, Drops, NS}' is returned.
%%
%% This function is slightly different from `queue:out_r/1', as the sojourn time
%% is included in the result in the place of the atom `value' and items can be
%% dropped.
%%
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
-spec out_r(Time, S) -> {Result, Drops, NS} when
      Time :: integer(),
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time), Time > PrevTime ->
    {Result, Drops, NQ, NState} = Module:handle_out_r(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    NS = S#squeue{time=Time, queue=NQ, state=NState},
    case Result of
        empty ->
            {empty, Drops2, NS};
        Item ->
            {sojourn_time(Time, Item), Drops2, NS}
    end;
out_r(Time, S) when is_integer(Time) ->
    out_r(S).

%% @equiv drop(time(S), S)
-spec drop(S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
drop(#squeue{time=Time} = S) ->
    drop(Time, S).

%% @doc Advances the queue, `S', to time `Time' and drops items, `Drops', from
%% the queue. Returns `{Drops, NS}', where `Drops' is the list of dropped items
%% and their sojourn times, and `NS' is the resulting queue without the dropped
%% items. If `S' is empty rauses an `empty' error.
%%
%% If the active queue management callback module does not drop any items, the
%% item at the head of the queue is dropped.
%%
%% This function is different from `queue:drop/1', as the return value is a
%% 2-tuple with the dropped items, `Drops' and the new queue , `NS', instead of
%% just the new queue.
%%
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
-spec drop(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
drop(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time), Time >= PrevTime ->
    case Module:handle_timeout(Time, Q, State) of
        {[], NQ, NState} ->
            case queue:out(NQ) of
                {empty, _} ->
                    error(empty, [Time, S]);
                {{value, Item}, NQ2} ->
                    NS = S#squeue{time=Time, queue=NQ2, state=NState},
                    {sojourn_drops(Time, [Item]), NS}
            end;
        {Drops, NQ, NState} ->
            NS = S#squeue{time=Time, queue=NQ, state=NState},
            {sojourn_drops(Time, Drops), NS}
    end;
drop(Time, #squeue{} = S) when is_integer(Time) ->
    error(badarg, [Time, S]).

%% @equiv drop_r(time(S), S)
-spec drop_r(S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
drop_r(#squeue{time=Time} = S) ->
    drop_r(Time, S).

%% @doc Advances the queue, `S', to time `Time' and drops items, `Drops', from
%% the queue. Returns `{Drops, NS}', where `Drops' is the list of dropped items
%% and their sojourn times, and `NS' is the resulting queue without the dropped
%% items. If `S' is empty rauses an `empty' error.
%%
%% If the active queue management callback module does not drop any items, the
%% item at the tail the queue is dropped.
%%
%% This function is different from `queue:drop_r/1', as the return value is a
%% 2-tuple with the dropped items, `Drops' and the new queue , `NS', instead of
%% just the new queue. Also the dropped item or items may not be from the tail
%% of the queue.
%%
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
-spec drop_r(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
drop_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time), Time >= PrevTime ->
    case Module:handle_timeout(Time, Q, State) of
        {[], NQ, NState} ->
            case queue:out_r(NQ) of
                {empty, _} ->
                    error(empty, [Time, S]);
                {{value, Item}, NQ2} ->
                    NS = S#squeue{time=Time, queue=NQ2, state=NState},
                    {sojourn_drops(Time, [Item]), NS}
            end;
        {Drops, NQ, NState} ->
            NS = S#squeue{time=Time, queue=NQ, state=NState},
            {sojourn_drops(Time, Drops), NS}
    end;
drop_r(Time, #squeue{} = S) when is_integer(Time) ->
    error(badarg, [Time, S]).

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
join(#squeue{module=Module1, time=Time, tail_time=TailTime1, queue=Q1,
             state=State1} = S1,
     #squeue{time=Time, tail_time=TailTime2, queue=Q2} = S2) ->
    case {queue:peek_r(Q1), queue:peek(Q2)} of
        {{value, {TailStart1, _}}, {value, {HeadStart2, _}}}
          when TailStart1 > HeadStart2 ->
            %% queues contain overlapping start times.
            error(badarg, [S1, S2]);
        _ ->
            NTailTime = max(TailTime1, TailTime2),
            NState1 = Module1:handle_join(Time, Q1, State1),
            NQ = queue:join(Q1, Q2),
            %% handle_join/1 is required to notify the queue manager that the
            %% max sojourn time of the queue may have increased (though only if
            %% the head queue was empty).
            S1#squeue{tail_time=NTailTime, queue=NQ, state=NState1}
    end;
join(#squeue{} = S1, #squeue{} = S2) ->
    error(badarg, [S1, S2]).

%% @equiv filter(time(S), Filter, S)
-spec filter(Filter, S) -> {Drops, NS} when
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
filter(Filter, #squeue{time=Time} = S) ->
    filter(Time, Filter, S).

%% @doc Advances the queue, `S', to time `Time'  and applies a filter fun,
%% `Filter', to all items in the queue. Returns a tuple containing the dropped
%% items and their sojourn times, `Drops', and the new queue, `NS'.
%%
%% If `Filter(Item)' returns `true', the item appears in the new queue.
%%
%% If `Filter(Item)' returns `false', the item does not appear in the new
%% queue or the dropped items.
%%
%% If `Filter(Item)' returns a list of items, these items appear in the new
%% queue with all items having the start time of the origin item, `Item'.
%%
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
-spec filter(Time, Filter, S) -> {Drops, NS} when
      Time :: integer(),
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
filter(Time, Filter,
       #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time), Time >= PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    NQ2 = queue:filter(make_filter(Filter), NQ),
    NS = S#squeue{time=Time, queue=NQ2, state=NState},
    {Drops2, NS};
filter(Time, Filter, #squeue{} = S) when is_integer(Time) ->
    error(badarg, [Time, Filter, S]).

%% Additional API

%% @doc Returns the current time, `Time', of the queue, `S'.
-spec time(S) -> Time when
      S :: squeue(),
      Time :: integer().
time(#squeue{time=Time}) ->
    Time.

%% @doc Advances the queue, `S', to time `Time', without applying active queue
%% management. Returns the new queue, `NS'.
%%
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
-spec time(Time, S) -> NS when
      Time :: integer(),
      S :: squeue(Item),
      NS :: squeue(Item).
time(Time, #squeue{time=Time} = S) ->
    S;
time(Time, #squeue{time=PrevTime} = S) when is_integer(Time), Time > PrevTime ->
    S#squeue{time=Time};
time(Time, #squeue{} = S) when is_integer(Time) ->
    error(badarg, [Time, S]).

%% @equiv timeout(time(S), S)
-spec timeout(S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
timeout(#squeue{time=Time} = S) ->
    timeout(Time, S).

%% @doc Advances the queue, `S', to time `Time'. Returns a tuple containing the
%% dropped items and their sojourn times, `Drops', and resulting queue, `NS'.
%%
%% This function raises the error `badarg' if `Time' is less than the current
%% time of the queue.
-spec timeout(Time, S) -> {Drops, NS} when
      Time :: integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
timeout(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time), Time >= PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {sojourn_drops(Time, Drops), S#squeue{time=Time, queue=NQ, state=NState}};
timeout(Time, #squeue{} = S) ->
    error(badarg, [Time, S]).

%% Test API

%% @hidden
from_start_list(Time, TailTime, List, Module, Args) ->
    S = new(Time, Module, Args),
    S#squeue{tail_time=TailTime, queue=queue:from_list(List)}.

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
