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
%% @doc Implements a head or tail drop queue with a timeout queue management
%% algorithm, where items are dropped once their sojourn time is greater than or
%% equal to a timeout value.
%%
%% `sbroker_timeout_queue' can be used as an `sbroker_queue' module in
%% `sbroker'. Its argument is of the form:
%% ```
%% {out | out_r, Timeout :: timeout(), drop | drop_r, Min :: non_neg_integer(),
%%  Max :: non_neg_integer() | infinity}.
%% '''
%% The first element is `out' for a FIFO queue and `out_r' for a LIFO queue.
%% `Timeout' is the timeout value in `milli_seconds', i.e. the minimum
%% sojourn time at which items are dropped from the queue when the queue is
%% above the minimum size `Min'. The third element determines whether to drop
%% from head (`drop') or drop from the tail (`drop_r') when the queue is above
%% the maximum size `Max'.
-module(sbroker_timeout_queue).

-behaviour(sbroker_queue).
-behaviour(sbroker_fair_queue).

%% public api

-export([init/3]).
-export([handle_in/5]).
-export([handle_out/2]).
-export([handle_fq_out/2]).
-export([handle_timeout/2]).
-export([handle_cancel/3]).
-export([handle_info/3]).
-export([config_change/3]).
-export([len/1]).
-export([terminate/2]).

%% types

-record(state, {out :: out | out_r,
                timeout :: timeout(),
                drop :: drop | drop_r,
                min :: non_neg_integer(),
                max :: non_neg_integer() | infinity,
                timeout_next :: integer() | infinity,
                len :: non_neg_integer(),
                queue :: sbroker_queue:internal_queue()}).

%% public api

%% @private
-spec init(Q, Time, {Out, Timeout, Drop, Min, Max}) -> {State, TimeoutNext} when
      Q :: sbroker_queue:internal_queue(),
      Time :: integer(),
      Out :: out | out_r,
      Timeout :: timeout(),
      Drop :: drop | drop_r,
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      State :: #state{},
      TimeoutNext :: integer() | infinity.
init(Q, Time, Args) ->
    handle_timeout(Time, from_queue(Q, queue:len(Q), Time, Args)).

%% @private
-spec handle_in(SendTime, From, Value, Time, State) ->
    {NState, TimeoutNext} when
      Time :: integer(),
      SendTime :: integer(),
      From :: {pid(), any()},
      Value :: any(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_in(SendTime, From, _, Time,
          #state{max=Max, len=Max, drop=drop_r} = State) ->
    sbroker_queue:drop(From, SendTime, Time),
    handle_timeout(Time, State);
handle_in(SendTime, {Pid, _} = From, Value, Time,
          #state{max=Max, len=Max, drop=drop, queue=Q,
                 timeout=Timeout} = State)  ->
    {{value, {SendTime2, _, _, _} = Item}, NQ} = queue:out(Q),
    drop_item(Time, Item),
    Ref = monitor(process, Pid),
    NQ2 = queue:in({SendTime, From, Value, Ref}, NQ),
    case Timeout of
        infinity ->
            {State#state{queue=NQ2}, infinity};
        _ ->
            in(SendTime2+Timeout, Max, NQ2, Time, State)
    end;
handle_in(SendTime, {Pid, _} = From, Value, Time,
          #state{len=Len, queue=Q, timeout_next=TimeoutNext} = State) ->
    Ref = monitor(process, Pid),
    NQ = queue:in({SendTime, From, Value, Ref}, Q),
    in(TimeoutNext, Len+1, NQ, Time, State).

%% @private
-spec handle_out(Time, State) ->
    {SendTime, From, Value, Ref, NState, TimeoutNext} | {empty, NState} when
      Time :: integer(),
      State :: #state{},
      SendTime :: integer(),
      From :: {pid(), any()},
      Value :: any(),
      Ref :: reference(),
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_out(_, #state{len=0} = State) ->
    {empty, State};
handle_out(Time, #state{out=out, len=Len, queue=Q} = State) ->
    out(queue:out(Q), Len-1, Time, State);
handle_out(Time, #state{out=out_r, len=Len, queue=Q} = State) ->
    out_r(queue:out_r(Q), Len-1, Time, State).

%% @private
-spec handle_fq_out(Time, State) ->
    {SendTime, From, Value, Ref, NState, NextTimeout} |
    {empty, NState, RemoveTime} when
      Time :: integer(),
      State :: #state{},
      SendTime :: integer(),
      From :: {pid(), any()},
      Value :: any(),
      Ref :: reference(),
      NState :: #state{},
      NextTimeout :: integer() | infinity,
      RemoveTime :: integer().
handle_fq_out(Time, State) ->
    case handle_out(Time, State) of
        {_, _, _, _, _, _} = Out ->
            Out;
        {empty, NState} ->
            {empty, NState, Time}
    end.

%% @private
-spec handle_timeout(Time, State) -> {State, TimeoutNext} when
      Time :: integer(),
      State :: #state{},
      TimeoutNext :: integer() | infinity.
handle_timeout(Time, #state{timeout_next=TimeoutNext, len=Len, min=Min} = State)
  when TimeoutNext > Time, Len > Min ->
    {State, TimeoutNext};
handle_timeout(Time, #state{len=Len, min=Min, queue=Q} = State)
  when Len > Min ->
    timeout(Min, Len, Q, Time, State);
handle_timeout(_, State) ->
    {State, infinity}.

%% @private
-spec handle_cancel(Tag, Time, State) -> {Cancelled, NState, TimeoutNext} when
      Tag :: any(),
      Time :: integer(),
      State :: #state{},
      Cancelled :: false | pos_integer(),
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_cancel(Tag, Time, #state{len=Len, queue=Q} = State) ->
    Cancel = fun({_, {_, Tag2}, _, Ref}) when Tag2 =:= Tag ->
                     demonitor(Ref, [flush]),
                     false;
                (_) ->
                     true
             end,
    NQ = queue:filter(Cancel, Q),
    NLen = queue:len(NQ),
    NState = State#state{len=NLen, queue=NQ},
    {NState2, TimeoutNext} = handle_timeout(Time, NState),
    case NLen of
        Len ->
            {false, NState2, TimeoutNext};
        _ ->
            {Len - NLen, NState2, TimeoutNext}
    end.

%% @private
-spec handle_info(Msg, Time, State) -> {NState, TimeoutNext} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_info({'DOWN', Ref, _, _, _}, Time, #state{queue=Q} = State) ->
    NQ = queue:filter(fun({_, _, _, Ref2}) -> Ref2 =/= Ref end, Q),
    handle_timeout(Time, State#state{len=queue:len(NQ), queue=NQ});
handle_info(_, Time, State) ->
    handle_timeout(Time, State).

-spec config_change({Out, Timeout, Drop, Min, Max}, Time, State) ->
    {NState, TimeoutNext} when
      Out :: out | out_r,
      Timeout :: timeout(),
      Drop :: drop | drop_r,
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
config_change(Arg, Time, #state{queue=Q, len=Len}) ->
    handle_timeout(Time, from_queue(Q, Len, Time, Arg)).

%% @private
-spec len(State) -> Len when
      State :: #state{},
      Len :: non_neg_integer().
len(#state{len=Len}) ->
    Len.

%% @private
-spec terminate(Reason, State) -> Q when
      Reason :: any(),
      State :: #state{},
      Q :: sbroker_queue:internal_queue().
terminate(_, #state{queue=Q}) ->
    Q.

%% Internal

first_timeout_next(_, infinity) ->
    infinity;
first_timeout_next(Time, Timeout)
  when is_integer(Timeout) andalso Timeout >= 0->
    Time.

in(TimeoutNext, Len, Q, Time, #state{min=Min} = State)
  when TimeoutNext > Time, Len > Min ->
    {State#state{len=Len, queue=Q, timeout_next=TimeoutNext}, TimeoutNext};
in(_, Len, Q, Time, #state{min=Min} = State) when Len > Min ->
    timeout(Min, Len, Q, Time, State);
in(TimeoutNext, Len, Q, _, State) ->
    {State#state{len=Len, queue=Q, timeout_next=TimeoutNext}, infinity}.

out({{value, {SendTime, From, Value, Ref}}, Q}, Len, _, #state{min=Min} = State)
  when Len < Min ->
    NState = State#state{queue=Q, len=Len},
    {SendTime, From, Value, Ref, NState#state{len=Len, queue=Q}, infinity};
out({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
    #state{timeout=Timeout} = State) when SendTime+Timeout > Time ->
    TimeoutNext = SendTime+Timeout,
    NState = State#state{queue=Q, len=Len, timeout_next=TimeoutNext},
    {SendTime, From, Value, Ref, NState, timeout_next(NState)};
out({{value, {SendTime, From, Value, Ref}}, Q}, Len, _,
    #state{timeout=infinity} = State) ->
    {SendTime, From, Value, Ref, State#state{len=Len, queue=Q}, infinity};
out({{value, {SendTime, _, _, _} = Item}, Q}, 0, Time,
    #state{timeout=Timeout} = State) ->
    drop_item(Time, Item),
    TimeoutNext = SendTime+Timeout,
    {empty, State#state{len=0, queue=Q, timeout_next=TimeoutNext}};
out({{value, Item}, Q}, Len, Time, State) ->
    drop_item(Time, Item),
    out(queue:out(Q), Len-1, Time, State).

out_r({{value, {SendTime, From, Value, Ref}}, Q}, Len, _,
    #state{min=Min} = State) when Len < Min ->
    NState = State#state{queue=Q, len=Len},
    {SendTime, From, Value, Ref, NState, infinity};
out_r({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
    #state{timeout_next=TimeoutNext} = State) when TimeoutNext > Time ->
    NState = State#state{queue=Q, len=Len},
    {SendTime, From, Value, Ref, NState, timeout_next(NState)};
out_r({{value, {SendTime, From, Value, Ref}}, Q}, 0, Time,
    #state{timeout=Timeout} = State) when SendTime+Timeout > Time ->
    {SendTime, From, Value, Ref, State#state{len=0, queue=Q}, infinity};
out_r({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
    #state{timeout=Timeout, min=Min} = State) when SendTime+Timeout > Time ->
    % Removed an item already
    {NState, TimeoutNext} = timeout(max(Min-1, 0), Len, Q, Time, State),
    {SendTime, From, Value, Ref, NState, TimeoutNext};
out_r({{value, {SendTime, From, Value, Ref}}, Q}, Len, _,
    #state{timeout=infinity} = State) ->
    {SendTime, From, Value, Ref, State#state{len=Len, queue=Q}, infinity};
out_r({{value, {SendTime, _, _, _} = Item}, Q}, _, Time,
      #state{min=0, timeout=Timeout} = State) ->
    drop_queue(Time, Q),
    drop_item(Time, Item),
    TimeoutNext=SendTime+Timeout,
    {empty, State#state{len=0, queue=queue:new(), timeout_next=TimeoutNext}};
out_r({{value, {SendTime, From, Value, Ref}}, Q}, _, Time,
      #state{min=1, timeout=Timeout} = State) ->
    drop_queue(Time, Q),
    TimeoutNext=SendTime+Timeout,
    NState = State#state{len=0, queue=queue:new(), timeout_next=TimeoutNext},
    {SendTime, From, Value, Ref, NState, infinity};
out_r({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
      #state{min=Min, timeout=Timeout} = State) ->
    NLen = Min-1,
    {DropQ, NQ} = queue:split(Len-NLen, Q),
    drop_queue(Time, DropQ),
    {SendTime2, _, _, _} = queue:get(NQ),
    NState = State#state{len=NLen, queue=NQ, timeout_next=SendTime2+Timeout},
    {SendTime, From, Value, Ref, NState, infinity}.

timeout_next(#state{len=Len, min=Min, timeout_next=TimeoutNext})
  when Len > Min ->
    TimeoutNext;
timeout_next(_) ->
    infinity.

timeout(Min, Len, Q, Time, #state{timeout=Timeout} = State) ->
    {SendTime, _, _, _} = Item = queue:get(Q),
    case SendTime+Timeout of
        TimeoutNext when TimeoutNext > Time, Len > Min ->
            NState = State#state{len=Len, queue=Q, timeout_next=TimeoutNext},
            {NState, TimeoutNext};
        TimeoutNext when TimeoutNext > Time ->
            {State#state{len=Len, queue=Q, timeout_next=TimeoutNext}, infinity};
        _ when Len-1 > Min ->
            drop_item(Time, Item),
            timeout(Min, Len-1, queue:drop(Q), Time, State);
        TimeoutNext ->
            drop_item(Time, Item),
            NState = State#state{len=Len-1, queue=queue:drop(Q),
                                 timeout_next=TimeoutNext},
            {NState, infinity}
    end.

from_queue(Q, Len, Time, {Out, Timeout, Drop, Min, infinity})
  when (Out =:= out orelse Out =:= out_r) andalso
       (Drop =:= drop orelse Drop =:= drop_r) ->
    NTimeout = sbroker_util:timeout(Timeout),
    {Min, infinity} = sbroker_util:min_max(Min, infinity),
    #state{out=Out, drop=Drop, min=Min, max=infinity, timeout=NTimeout,
           timeout_next=first_timeout_next(Time, NTimeout), len=Len, queue=Q};
from_queue(Q, Len, Time, {Out, Timeout, drop, Min, 0}) ->
    from_queue(Q, Len, Time, {Out, Timeout, drop_r, Min, 0});
from_queue(Q, Len, Time, {Out, Timeout, Drop, Min, Max})
  when (Out =:= out orelse Out =:= out_r) andalso
       (Drop =:= drop orelse Drop =:= drop_r) ->
    {Min, Max} = sbroker_util:min_max(Min, Max),
    NTimeout = sbroker_util:timeout(Timeout),
    Next = first_timeout_next(Time, NTimeout),
    State = #state{out=Out, drop=Drop, min=Min, max=Max, timeout=NTimeout,
                   timeout_next=Next, len=Len, queue=Q},
    case Len - Max of
        DropCount when DropCount > 0 andalso Drop =:= drop ->
            {DropQ, NQ} = queue:split(DropCount, Q),
            _ = drop_queue(Time, DropQ),
            State#state{len=Max, queue=NQ};
        DropCount when DropCount > 0 andalso Drop =:= drop_r ->
            {NQ, DropQ} = queue:split(Max, Q),
            _ = drop_queue(Time, DropQ),
            State#state{len=Max, queue=NQ};
        _ ->
            State
    end.

drop_queue(Time, Q) ->
    Drop = fun({SendTime, From, _, Ref}) ->
                   demonitor(Ref, [flush]),
                   sbroker_queue:drop(From, SendTime, Time),
                   false
           end,
    queue:filter(Drop, Q).

drop_item(Time, {SendTime, From, _, Ref}) ->
    demonitor(Ref, [flush]),
    sbroker_queue:drop(From, SendTime, Time).
