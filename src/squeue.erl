%% @doc
%% This module provides sojourn-time based active queue management with a
%% similar API to OTP's `queue' module.
%%
%% A subset of the `squeue' API is the `queue' module's API with one exception:
%% when `{value, Item}' is returned by `queue', `squeue' returns
%% `{SojournTime, Item}', where `SojournTime' (`non_neg_integer()') is the
%% sojourn time of the item (or length of time an item waited in the queue).
%%
%% `squeue' also provides an optional first argument to all functions in common
%% with `queue': `Time'. This argument is of type `non_neg_integer()' and sets
%% the current time of the queue, if `Time' is greater than (or equal) to
%% the queue's previous time. When the `Time' argument is included items
%% may be dropped by the queue's management algorithm. The dropped items
%% will be included in the return value when the queue itself is also
%% returned. The dropped items are a list of the form: `[{SojournTime, Item}]',
%% which is ordered with the item with the greatest `SojournTime' (i.e. the
%% oldest) at the head. If the `Time' argument is equal to the current time
%% of the queue no items are dropped but an empty list is still returned.
%%
%% `squeue' includes 3 queue management algorithms: `squeue_naive',
%% `squeue_timeout', `squeue_codel'.
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
                 queue = queue:new() :: queue:queue()}).
-type squeue() :: squeue(any()).
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

%% @doc Returns a tuple containing an empty list of dropped items, `Drops', and
%% an empty queue, `S', with the `squeue_naive' management algorithm, and time
%% of `Time'.
-spec new(Time) -> {Drops, S} when
      Time :: non_neg_integer(),
      Drops :: [],
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

%% @doc Returns a tuple containing an empty list of dropped items, `Drops', and
%% an empty queue, `S', with the `Module' management algorithm started with
%% arguments `Args' and time of `Time'.
-spec new(Time, Module, Args) -> {Drops, S} when
      Time :: non_neg_integer(),
      Module :: module(),
      Args :: any(),
      Drops :: [],
      S :: squeue().
new(Time, Module, Args) when is_integer(Time) andalso Time >= 0 ->
    State = Module:init(Args),
    {[], #squeue{module=Module, time=Time, state=State}}.

%% @doc Tests if a term, `Term', is an `squeue' queue, returns `true' if is,
%% otherwise `false'.
-spec is_queue(Term) -> Bool when
      Term :: any(),
      Bool :: boolean().
is_queue(#squeue{}) ->
    true;
is_queue(_Other) ->
    false.

%% @doc Tests if a term, `Term', is an `squeue' queue, returns `true' if is,
%% otherwise `false'.
%%
%% This function is included for completeness and {@link is_queue/1} should be
%% used.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
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

%% @doc Tests if a queue, `S', is empty, returns `true' if is, otherwise
%% `false'.
-spec is_empty(S) -> Bool when
      S :: squeue(),
      Bool :: boolean().
is_empty(#squeue{queue=Q}) ->
    queue:is_empty(Q).

%% @doc Tests if a queue, `S', will be empty at time `Time', returns `true' if
%% it will be, otherwise `false'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec is_empty(Time, S) -> Bool when
      Time :: non_neg_integer(),
      S :: squeue(),
      Bool :: boolean().
is_empty(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_timeout(Time, Q, State),
    queue:is_empty(NQ);
is_empty(Time, #squeue{time=Time} = S) ->
    is_empty(S);
is_empty(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Returns the length of the queue, `S'.
-spec len(S) -> Len when
      S :: squeue(),
      Len :: non_neg_integer().
len(#squeue{queue=Q}) ->
    queue:len(Q).

%% @doc Returns the length of the queue, `S', at time `Time'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec len(Time, S) -> Len when
      Time :: non_neg_integer(),
      S :: squeue(),
      Len :: non_neg_integer().
len(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_timeout(Time, Q, State),
    queue:len(NQ);
len(Time, #squeue{time=Time} = S) ->
    len(S);
len(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Inserts the item, `Item', at the tail of queue, `S'. Returns the
%% resulting queue, `NS'.
-spec in(Item, S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
in(Item, #squeue{time=Time, queue=Q} = S) ->
    S#squeue{queue=queue:in({Time, Item}, Q)}.

%% @doc Advances the queue, `S', to time `Time' and drops item, then inserts
%% the item, `Item', at the tail of queue, `S'. Returns a tuple containing the
%% dropped items and their sojourn times, `Drops', and resulting queue, `NS'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec in(Time, Item, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
in(Time, Item, #squeue{module=Module, state=State, time=PrevTime, queue=Q} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    NS = S#squeue{time=Time, queue=queue:in({Time, Item}, NQ), state=NState},
    {sojourn_drops(Time, Drops), NS};
in(Time, Item, #squeue{time=Time} = S) ->
    {[], in(Item, S)};
in(Time, Item, #squeue{} = S) ->
    error(badtime, [Time, Item, S]).

%% @doc Inserts the item, `Item', at the head of queue, `S'. Returns the
%% resulting queue, `NS'.
%%
%% This function raises the error `badtime' if current head of queue `S' was
%% added before the current time.
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

%% @doc Advances the queue, `S', to time `Time' and drops items, then inserts
%% the item, `Item', at the head of queue, `S'. Returns a tuple containing the
%% dropped items and their sojourn times, `Drops', and resulting queue, `NS'.
%%
%% This function raises the error `badtime' if current head of queue `S' was
%% added before the current time.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec in_r(Time, Item, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
in_r(Time, Item,
     #squeue{module=Module, state=State, time=PrevTime, queue=Q} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    NS = in_r(Item, S#squeue{time=Time, queue=NQ, state=NState}),
    {sojourn_drops(Time, Drops), NS};
in_r(Time, Item, #squeue{time=Time} = S) ->
    {[], in_r(Item, S)};
in_r(Time, Item, #squeue{} = S) ->
    error(badtime, [Time, Item, S]).

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
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec out(Time, S) -> {Result, Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {Drops2, NQ2, NState2} = Module:handle_out(Time, NQ, NState),
    Drops3 = sojourn_drops(Time, Drops, sojourn_drops(Time, Drops2)),
    {Result, NQ3} = queue:out(NQ2),
    NS = S#squeue{time=Time, queue=NQ3, state=NState2},
    case Result of
        empty ->
            {empty, Drops3, NS};
        {value, Item} ->
            {sojourn_time(Time, Item), Drops3, NS}
    end;
out(Time, #squeue{time=Time} = S) ->
    out(S);
out(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

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
    {Drops, NQ, NState} = Module:handle_out(Time, Q, State),
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
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec out_r(Time, S) -> {Result, Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Result :: empty | {SojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
out_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {Drops2, NQ2, NState2} = Module:handle_out(Time, NQ, NState),
    Drops3 = sojourn_drops(Time, Drops, sojourn_drops(Time, Drops2)),
    {Result, NQ3} = queue:out_r(NQ2),
    NS = S#squeue{time=Time, queue=NQ3, state=NState2},
    case Result of
        empty ->
            {empty, Drops3, NS};
        {value, Item} ->
            {sojourn_time(Time, Item), Drops3, NS}
    end;
out_r(Time, #squeue{time=Time} = S) ->
    out_r(S);
out_r(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Returns a queue, `S', containing the list of items in `List'. The queue,
%% `S', will use `squeue_naive' to manage the queue and be at time `0'.
-spec from_list(List) -> S when
      List :: [Item],
      S :: squeue(Item).
from_list(List) ->
    from_list(List, squeue_naive, undefined).

%% @doc Returns a tuple containing an empty list of dropped items and a queue,
%% `S', containing the list of items in `List'. The queue, `S', will use
%% `squeue_naive' to manage the queue and be atime `Time'.
-spec from_list(Time, List) -> {Drop, S} when
      Time :: non_neg_integer(),
      List :: [Item],
      Drop :: [],
      S :: squeue(Item).
from_list(Time, List) ->
    from_list(Time, List, squeue_naive, undefined).

%% @doc Returns a queue, `S', containing the list of items in `List'. The queue,
%% `S', will use `Module' with arguments `Args' to manage the queue and be at
%% time `0'.
-spec from_list(List, Module, Args) -> S when
      List :: [Item],
      Module :: module(),
      Args :: any(),
      S :: squeue(Item).
from_list(List, Module, Args) ->
    {[], S} = from_list(0, List, Module, Args),
    S.

%% @doc Returns a tuple containing an empty list of droped items and a queue,
%% `S', containing the list of items in `List'. The queue, `S', will use `Module'
%% with arguments `Args' to manage the queue and be at time `Time'.
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

%% @doc Returns a list of items, `List', in the queue, `S'.
%%
%% The order of items in `List' matches their order in the queue, `S', so that
%% the item at the head of the queue is at the head of the list.
-spec to_list(S) -> List when
      S :: squeue(Item),
      List :: [Item].
to_list(#squeue{queue=Q}) ->
    [Item || {_Start, Item} <- queue:to_list(Q)].

%% @doc Advances the queue, `S', to time `Time' and returns a list `List' of the
%% items remaining in the queue.
%%
%% The order of items in `List' matches their order in the queue, `S', so that
%% the item at the head of the queue is at the head of the list.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec to_list(Time, S) -> List when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      List :: [Item].
to_list(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_timeout(Time, Q, State),
    [Item || {_Start, Item} <- queue:to_list(NQ)];
to_list(Time, #squeue{time=Time} = S) ->
    to_list(S);
to_list(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Returns a new queue, `NS', that contains the items of queue `S' in
%% reverse order.
%%
%% This function raises the error `badtime' if all items in queue `S' were not
%% added at the same time.
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

%% @doc Advances the queue, `S' to time `Time' and drops items, then reverses
%% the order of the remaining items in the queue. Returns a tuple containing the
%% dropped items and their sojourn times, `Drops', and the reversed queue, `NS'.
%%
%% This function raises the error `badtime' if all items in queue `S' at time
%% `Time' were not added at the same time.
%%
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec reverse(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
reverse(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    {Drops2, reverse(S#squeue{time=Time, queue=NQ, state=NState})};
reverse(Time, #squeue{time=Time} = S) ->
    {[],  reverse(S)};
reverse(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Splits the queue `S' in two, with the `N' items at head of `S' in one
%% queue, `S1', and the rest of the items in `S2'.
%%
%% Both returned queues, `S1' and `S2', will maintain the queue management state
%% of queue `S'.
-spec split(N, S) -> {Drops, S1, S2} when
      N :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      S1 :: squeue(Item),
      S2 :: squeue(Item).
split(N, #squeue{module=Module, time=Time, queue=Q, state=State} = S) ->
    {Drops, NQ, NState} = Module:handle_out(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    {Q1, Q2} = queue:split(N, NQ),
    S1 = S#squeue{queue=Q1, state=NState},
    S2 = S#squeue{queue=Q2, state=NState},
    {Drops2, S1, S2}.

%% @doc Advances the queue, `S', to time `Time' and drops items, then splits the
%% remaing queue in two, with the `N' items at head in one queue, `S1', and the
%% rest of the items in `S2'. Returns a tuple containing the dropped items and
%% their sojourn times, `Drops', and the two new queues, `S1' and `S2'.
%%
%% Both returned queues, `S1' and `S2', will maintain the queue management state
%% of queue `S' at time `Time'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec split(Time, N, S) -> {Drops, S1, S2} when
      Time :: non_neg_integer(),
      N :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      S1 :: squeue(Item),
      S2 :: squeue(Item).
split(Time, N, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {Drops2, NQ2, NState2} = Module:handle_out(Time, NQ, NState),
    Drops3 = sojourn_drops(Time, Drops, sojourn_drops(Time, Drops2)),
    {Q1, Q2} = queue:split(N, NQ2),
    S1 = S#squeue{time=Time, queue=Q1, state=NState2},
    S2 = S#squeue{time=Time, queue=Q2, state=NState2},
    {Drops3, S1, S2};
split(Time, N, #squeue{time=Time} = S) ->
    split(N, S);
split(Time, N, #squeue{} = S) ->
    error(badtime, [Time, N, S]).

%% @doc Joins two queues, `S1' and `S2', into one queue, `NS', with the items in
%% `S1' at the head and the items in `S2' at the tail.
%%
%% This function raises the error `badtime' if any item in queue `S1' was added
%% after any item in queue `S2'.
%%
%% This function raises the error `badtime' if the current time of the queues,
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
            error(badtime, [S1, S2]);
        _ ->
            {[], NQ1, NState1} = Module1:handle_join(Time, Q1, State1),
            NQ = queue:join(NQ1, Q2),
            %% handle_join/1 is required to notify the queue manager that the
            %% max sojourn time of the queue may have increased (though only if
            %% the head queue was empty).
            S1#squeue{queue=NQ, state=NState1}
    end;
join(#squeue{} = S1, #squeue{} = S2) ->
    error(badtime, [S1, S2]).


%%% @doc Advances the queues, `S1' and `S2', to time `Time' and drops items,
%%% then joins the remaining items in the two queues into one queue, `NS', with
%%% the remaining items from `S1' at the head and the remaining items from `S2'
%%% at the tail. Returns a tuple containing the dropped items and their sojourn
%%% times, `Drops', and the new queue `NS'.
%%
%% This function raises the error `badtime' if any remaining item in queue `S1'
%% at time `Time' was added after any remaining item in queue `S2' at time
%% `Time'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of either queue, `S1' or `S2'.
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
    {Drops1, NQ1, NState1} = Module1:handle_timeout(Time, Q1, State1),
    NS1 = S1#squeue{time=Time, queue=NQ1, state=NState1},
    {Drops2, NQ2, NState2} = Module2:handle_timeout(Time, Q2, State2),
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
    {Drops, NQ2, NState2} = Module2:handle_timeout(Time1, Q2, State2),
    NS2 = S2#squeue{time=Time1, queue=NQ2, state=NState2},
    {sojourn_drops(Time1, Drops), join(S1, NS2)};
join(Time2, #squeue{module=Module1, time=Time1, queue=Q1, state=State1} = S1,
     #squeue{time=Time2} = S2) when Time2 > Time1 ->
    {Drops, NQ1, NState1} = Module1:handle_timeout(Time2, Q1, State1),
    NS1 = S1#squeue{time=Time2, queue=NQ1, state=NState1},
    {sojourn_drops(Time2, Drops), join(NS1, S2)};
join(Time, #squeue{} = S1, #squeue{} = S2) ->
    error(badtime, [Time, S1, S2]).

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
    {Drops, NQ, NState} = Module:handle_out(Time, Q, State),
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
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec filter(Time, Filter, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      Filter :: fun((Item) -> Bool :: boolean() | [Item]),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
filter(Time, Filter,
       #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {Drops2, NQ2, NState2} = Module:handle_out(Time, NQ, NState),
    Drops3 = sojourn_drops(Time, Drops, sojourn_drops(Time, Drops2)),
    NQ3 = queue:filter(make_filter(Filter), NQ2),
    NS = S#squeue{time=Time, queue=NQ3, state=NState2},
    {Drops3, NS};
filter(Time, Filter, #squeue{time=Time} = S) ->
    filter(Filter, S);
filter(Time, Filter, #squeue{} = S) ->
    error(badtime, [Time, Filter, S]).

%% @doc Tests if an item, `Item', is in the queue, `S', returns `true' if it is,
%% otherwise `false'.
-spec member(Item, S) -> Bool when
      S :: squeue(Item),
      Bool :: boolean().
member(Item, #squeue{queue=Q}) ->
    NQ = queue:filter(fun({_InTime, Item2}) -> Item2 =:= Item end, Q),
    not queue:is_empty(NQ).

%% @doc Tests if an item, `Item', is in the queue, `S', at time `Time', returns
%% `true' if it will be, otherwise `false'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec member(Time, Item, S) -> Bool when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Bool :: boolean().
member(Time, Item,
       #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, _NState} = Module:handle_timeout(Time, Q, State),
    NQ2 = queue:filter(fun({_InTime, Item2}) -> Item2 =:= Item end, NQ),
    not queue:is_empty(NQ2);
member(Time, Item, #squeue{time=Time} = S) ->
    member(Item, S);
member(Time, Item, #squeue{} = S) ->
    error(badtime, [Time, Item, S]).

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
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec timeout(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
timeout(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {sojourn_drops(Time, Drops), S#squeue{time=Time, queue=NQ, state=NState}};
timeout(Time, #squeue{time=Time} = S) ->
    {[], S};
timeout(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% Extended API

%% @doc Drops items and returns the item, `Item', at the head of the queue, `S'.
%%
%% This functions raise the error `empty' if the queue, `S', is empty after
%% dropping items.
-spec get(S) -> Item when
      S :: squeue(Item).
get(#squeue{module=Module, time=Time, queue=Q, state=State}) ->
    {_Drops, NQ, _NState} = Module:handle_out(Time, Q, State),
    {_InTime, Item} = queue:get(NQ),
    Item.

%% @doc Returns the item, `Item', at the head of the queue, `S', at time `Time'
%% after dropping items.
%%
%% This functions raise the error `empty' if the queue, `S', is empty after
%% dropping items at time `Time'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec get(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
get(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {_Drops2, NQ2, _NState2} = Module:handle_out(Time, NQ, NState),
    {_InTime, Item} = queue:get(NQ2),
    Item;
get(Time, #squeue{time=Time} = S) ->
    get(S);
get(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Drops items and returns the item, `Item', at the tail of the queue, `S'.
%%
%% This functions raise the error `empty' if the queue, `S', is empty after
%% dropping items.
-spec get_r(S) -> Item when
      S :: squeue(Item).
get_r(#squeue{module=Module, time=Time, queue=Q, state=State}) ->
    {_Drops, NQ, _NState} = Module:handle_out(Time, Q, State),
    {_InTime, Item} = queue:get_r(NQ),
    Item.

%% @doc Returns the item, `Item', at the tail of the queue, `S', at time `Time',
%% after dropping items.
%%
%% This functions raise the error `empty' if the queue, `S', is empty after
%% dropping items at time `Time'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec get_r(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
get_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {_Drops2, NQ2, _NState2} = Module:handle_out(Time, NQ, NState),
    {_InTime, Item} = queue:get_r(NQ2),
    Item;
get_r(Time, #squeue{time=Time} = S) ->
    get_r(S);
get_r(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Drops items and returns the item, `Item', and its sojourn time,
%% `SojournTime', from the head of the queue, `S'. If the queue, `S', is empty
%% after dropping items returns `empty'.
%%
%% This function is slightly different from `queue:peek/1', as the sojourn time
%% is included in the result in the place of the atom `value'.
-spec peek(S) -> empty | {SojournTime, Item} when
      S :: squeue(Item),
      SojournTime :: non_neg_integer().
peek(#squeue{module=Module, time=Time, queue=Q, state=State}) ->
    {_Drops, NQ, _NState} = Module:handle_out(Time, Q, State),
    case queue:peek(NQ) of
        empty ->
            empty;
        {value, Result} ->
            sojourn_time(Time, Result)
    end.

%% @doc Returns the item, `Item' and its sojourn time, `SojournTime', from the
%% head of the queue, `S', at time `Time', after dropping items. If the queue,
%% `S', is empty at after dropping items at time `Time', returns `empty'.
%%
%% This function is slightly different from `queue:peek/1', as the sojourn time
%% is included in the result in the place of the atom `value'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec peek(Time, S) -> empty | {SojournTime, Item} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      SojournTime :: non_neg_integer().
peek(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {_Drops2, NQ2, _NState2} = Module:handle_out(Time, NQ, NState),
    case queue:peek(NQ2) of
        empty ->
            empty;
        {value, Result} ->
            sojourn_time(Time, Result)
    end;
peek(Time, #squeue{time=Time} = S) ->
    peek(S);
peek(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Drops items and returns the item, `Item', and its sojourn time,
%% `SojournTime', from the tail of the queue, `S'. If the queue, `S', is empty
%% after dropping items returns `empty'.
%%
%% This function is slightly different from `queue:peek_r/1', as the sojourn
%% time is included in the result in the place of the atom `value'.
-spec peek_r(S) -> empty | {SojournTime, Item} when
      S :: squeue(Item),
      SojournTime :: non_neg_integer().
peek_r(#squeue{module=Module, time=Time, queue=Q, state=State}) ->
    {_Drops, NQ, _NState} = Module:handle_out(Time, Q, State),
    case queue:peek_r(NQ) of
        empty ->
            empty;
        {value, Result} ->
            sojourn_time(Time, Result)
    end.

%% @doc Returns the item, `Item' and its sojourn time, `SojournTime', from the
%% tail of the queue, `S', at time `Time', after dropping items. If the queue,
%% `S', is empty after dropping items at time `Time', returns `empty'.
%%
%% This function is slightly different from `queue:peek_r/1', as the sojourn
%% time is included in the result in the place of the atom `value'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec peek_r(Time, S) -> empty | {SojournTime, Item} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      SojournTime :: non_neg_integer().
peek_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State})
  when is_integer(Time) andalso Time > PrevTime ->
    {_Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {_Drops2, NQ2, _NState2} = Module:handle_out(Time, NQ, NState),
    case queue:peek_r(NQ2) of
        empty ->
            empty;
        {value, Result} ->
            sojourn_time(Time, Result)
    end;
peek_r(Time, #squeue{time=Time} = S) ->
    peek_r(S);
peek_r(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Drops items, `Drops', and removes the item from the head of the queue,
%% `S'. Returns {`Drops', `NS'}, where `Drops' is a list of dropped items and
%% their sojourn times and `NS' is the resulting queue after dropping items and
%% the removing the head of the queue.
%%
%% This function raises the error `empty' if the queue, `S', is empty after
%% dropping items.
-spec drop(S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
drop(#squeue{module=Module, time=Time, queue=Q, state=State} = S) ->
    {Drops, NQ, NState} = Module:handle_out(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    {Drops2, S#squeue{time=Time, queue=queue:drop(NQ), state=NState}}.

%% @doc Advances the queue, `S', to time `Time' and drops item, then removes the
%% item from the head of the new queue. Returns a tuple containing the dropped
%% items and their sojourn times, `Drops', and resulting queue, `NS'.
%%
%% This functions raise the error `empty' if the queue, `S', is empty after
%% dropping items at time `Time'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec drop(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
drop(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {Drops2, NQ2, NState2} = Module:handle_out(Time, NQ, NState),
    Drops3 = sojourn_drops(Time, Drops, sojourn_drops(Time, Drops2)),
    {Drops3, S#squeue{time=Time, queue=queue:drop(NQ2), state=NState2}};
drop(Time, #squeue{time=Time} = S) ->
    drop(S);
drop(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% @doc Drops items, `Drops', and removes the item from the tail of the queue,
%% `S'. Returns {`Drops', `NS'}, where `Drops' is a list of dropped items and
%% their sojourn times and `NS' is the resulting queue after dropping items and
%% the removing the tail of the queue.
%%
%% This function raises the error `empty' if the queue, `S', is empty after
%% dropping items.
-spec drop_r(S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
drop_r(#squeue{module=Module, time=Time, queue=Q, state=State} = S) ->
    {Drops, NQ, NState} = Module:handle_out(Time, Q, State),
    Drops2 = sojourn_drops(Time, Drops),
    {Drops2, S#squeue{time=Time, queue=queue:drop_r(NQ), state=NState}}.

%% @doc Advances the queue, `S', to time `Time' and drops item, then removes the
%% item from the tail of the new queue. Returns a tuple containing the dropped
%% items and their sojourn times, `Drops', and resulting queue, `NS'.
%%
%% This functions raise the error `empty' if the queue, `S', is empty after
%% dropping items at time `Time'.
%%
%% This function raises the error `badtime' if `Time' is not a valid time
%% greater than or equal to the current time of the queue, `S'.
-spec drop_r(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
drop_r(Time, #squeue{module=Module, time=PrevTime, queue=Q, state=State} = S)
  when is_integer(Time) andalso Time > PrevTime ->
    {Drops, NQ, NState} = Module:handle_timeout(Time, Q, State),
    {Drops2, NQ2, NState2} = Module:handle_out(Time, NQ, NState),
    Drops3 = sojourn_drops(Time, Drops, sojourn_drops(Time, Drops2)),
    {Drops3, S#squeue{time=Time, queue=queue:drop_r(NQ2), state=NState2}};
drop_r(Time, #squeue{time=Time} = S) ->
    drop_r(S);
drop_r(Time, #squeue{} = S) ->
    error(badtime, [Time, S]).

%% Okasaki API

%% @equiv in(Item, S)
-spec cons(Item, S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
cons(Item, S) ->
    in(Item, S).

%% @equiv in(Time, Item, S)
-spec cons(Time, Item, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
cons(Time, Item, S) ->
    in(Time, Item, S).

%% @equiv get(S)
-spec head(S) -> Item when
      S :: squeue(Item).
head(S) ->
    get(S).

%% @equiv get(Time, S)
-spec head(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
head(Time, S) ->
    get(Time, S).

%% @equiv drop(S)
-spec tail(S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
tail(S) ->
    drop(S).

%% @equiv drop(Time, S)
-spec tail(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
tail(Time, S) ->
    drop(Time, S).

%% @equiv in_r(Item, S)
-spec snoc(Item, S) -> NS when
      S :: squeue(Item),
      NS :: squeue(Item).
snoc(Item, S) ->
    in_r(Item, S).

%% @equiv in_r(Time, Item, S)
-spec snoc(Time, Item, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{SojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
snoc(Time, Item, S) ->
    in_r(Time, Item, S).

%% @equiv get_r(S)
-spec daeh(S) -> Item when
      S :: squeue(Item).
daeh(S) ->
    get_r(S).

%% @equiv get_r(Time, S)
-spec daeh(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
daeh(Time, S) ->
    get_r(Time, S).

%% @equiv get_r(S)
-spec last(S) -> Item when
      S :: squeue(Item).
last(S) ->
    get_r(S).

%% @equiv get_r(Time, S)
-spec last(Time, S) -> Item when
      Time :: non_neg_integer(),
      S :: squeue(Item).
last(Time, S) ->
    get_r(Time, S).

%% @equiv drop_r(S)
-spec liat(S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue(Item).
liat(S) ->
    drop_r(S).

%% @equiv drop_r(Time, S)
-spec liat(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
liat(Time, S) ->
    drop_r(Time, S).

%% @equiv drop_r(S)
-spec init(S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
init(S) ->
    drop_r(S).

%% @equiv drop_r(Time, S)
-spec init(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
init(Time, S) ->
    drop_r(Time, S).

%% Misspelt Okasaki API

%% @equiv drop_r(S)
-spec lait(S) -> {Drops, NS} when
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
lait(S) ->
    drop_r(S).

%% @equiv drop_r(Time, S)
-spec lait(Time, S) -> {Drops, NS} when
      Time :: non_neg_integer(),
      S :: squeue(Item),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue().
lait(Time, S) ->
    drop_r(Time, S).

%% Test API

%% @hidden
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
