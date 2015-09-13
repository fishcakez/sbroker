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
%% {out | out_r, Timeout :: timeout(), drop | drop_r,
%% Max :: non_neg_integer() | infinity}.
%% '''
%% The first element is `out' for a FIFO queue and `out_r' for a LIFO queue. The
%% second element is the timeout value, i.e. the minimum sojourn time at which
%% items are dropped from the queue. The third element determines whether to
%% drop from head (`drop') or drop from the tail (`drop_r') when the queue is
%% above the maximum size (fourth element).
-module(sbroker_timeout_queue).

-behaviour(sbroker_queue).

%% public api

-export([init/2]).
-export([handle_in/5]).
-export([handle_out/2]).
-export([handle_timeout/2]).
-export([handle_cancel/3]).
-export([handle_info/3]).
-export([config_change/3]).
-export([to_list/1]).
-export([len/1]).
-export([terminate/2]).

%% types

-ifdef(LEGACY_TYPES).
-type internal_queue() :: queue().
-else.
-type internal_queue() ::
    queue:queue({integer(), {pid(), any()}, any(), reference()}).
-endif.

-record(state, {out :: out | out_r,
                timeout :: timeout(),
                drop :: drop | drop_r,
                max :: non_neg_integer() | infinity,
                timeout_next :: integer() | infinity,
                len = 0 :: non_neg_integer(),
                queue = queue:new() :: internal_queue()}).

%% public api

%% @private
-spec init(Time, {Out, Timeout, Drop, Max}) -> State when
      Time :: integer(),
      Out :: out | out_r,
      Timeout :: timeout(),
      Drop :: drop | drop_r,
      Max :: non_neg_integer() | infinity,
      State :: #state{}.
init(Time, {Out, Timeout, drop, 0}) ->
    init(Time, {Out, Timeout, drop_r, 0});
init(Time, {Out, Timeout, Drop, Max})
  when (Out =:= out orelse Out =:= out_r) andalso
       (Drop =:= drop orelse Drop =:= drop_r) andalso
       ((is_integer(Max) andalso Max >= 0) orelse Max =:= infinity) ->
    TimeoutNext = first_timeout_next(Time, Timeout),
    #state{out=Out, timeout=Timeout, drop=Drop, max=Max,
           timeout_next=TimeoutNext}.

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
handle_in(SendTime, {Pid, _} = From, Value, Time, State) ->
    case timeout(Time, State) of
        #state{max=Max, len=Max, drop=drop_r,
               timeout_next=TimeoutNext} = NState ->
            sbroker_queue:drop(From, SendTime, Time),
            {NState, TimeoutNext};
        #state{max=Max, len=Max, drop=drop, queue=Q,
               timeout_next=TimeoutNext} = NState ->
            {{value, Item}, NQ} = queue:out(Q),
            drop_item(Time, Item),
            Ref = monitor(process, Pid),
            NQ2 = queue:in({SendTime, From, Value, Ref}, NQ),
            {NState#state{queue=NQ2}, TimeoutNext};
        #state{len=0, queue=Q, timeout=Timeout} = NState ->
            Ref = monitor(process, Pid),
            NQ = queue:in({SendTime, From, Value, Ref}, Q),
            TimeoutNext = max(Time, timeout_next(SendTime, Timeout)),
            NState2 = NState#state{len=1, queue=NQ, timeout_next=TimeoutNext},
            {NState2, TimeoutNext};
        #state{len=Len, queue=Q, timeout_next=TimeoutNext} = NState ->
            Ref = monitor(process, Pid),
            NQ = queue:in({SendTime, From, Value, Ref}, Q),
            {NState#state{len=Len+1, queue=NQ}, TimeoutNext}
    end.

%% @private
-spec handle_out(Time, State) ->
    {SendTime, From, Value, NState, TimeoutNext} | {empty, NState} when
      Time :: integer(),
      State :: #state{},
      SendTime :: integer(),
      From :: {pid(), any()},
      Value :: any(),
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_out(Time, State) ->
    case timeout(Time, State) of
        #state{len=0} = NState ->
            {empty, NState};
        #state{out=out, len=Len, queue=Q, timeout_next=TimeoutNext} = NState ->
            {{value, {SendTime, From, Value, Ref}}, NQ} = queue:out(Q),
            demonitor(Ref, [flush]),
            NState2 = NState#state{len=Len-1, queue=NQ},
            {SendTime, From, Value, NState2, TimeoutNext};
        #state{out=out_r, len=Len, queue=Q,
               timeout_next=TimeoutNext} = NState ->
            {{value, {SendTime, From, Value, Ref}}, NQ} = queue:out_r(Q),
            demonitor(Ref, [flush]),
            NState2 = NState#state{len=Len-1, queue=NQ},
            {SendTime, From, Value, NState2, TimeoutNext}
    end.

%% @private
-spec handle_timeout(Time, State) -> {State, TimeoutNext} when
      Time :: integer(),
      State :: #state{},
      TimeoutNext :: integer() | infinity.
handle_timeout(Time, State) ->
    #state{timeout_next=TimeoutNext} = NState = timeout(Time, State),
    {NState, TimeoutNext}.

%% @private
-spec handle_cancel(Tag, Time, State) -> {Cancelled, NState, TimeoutNext} when
      Tag :: any(),
      Time :: integer(),
      State :: #state{},
      Cancelled :: false | pos_integer(),
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_cancel(Tag, Time, State) ->
    #state{len=Len, queue=Q, timeout_next=TimeoutNext} = NState =
        timeout(Time, State),
    Cancel = fun({_, {_, Tag2}, _, Ref}) when Tag2 =:= Tag ->
                     demonitor(Ref, [flush]),
                     false;
                (_) ->
                     true
             end,
    NQ = queue:filter(Cancel, Q),
    case queue:len(NQ) of
        Len ->
            {false, NState, TimeoutNext};
        NLen ->
            {Len - NLen, NState#state{len=NLen, queue=NQ}, TimeoutNext}
    end.

%% @private
-spec handle_info(Msg, Time, State) -> {NState, TimeoutNext} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_info({'DOWN', Ref, _, _, _}, Time, State) ->
    #state{queue=Q, timeout_next=TimeoutNext} = NState = timeout(Time, State),
    NQ = queue:filter(fun({_, _, _, Ref2}) -> Ref2 =/= Ref end, Q),
    {NState#state{len=queue:len(NQ), queue=NQ}, TimeoutNext};
handle_info(_, Time, State) ->
    handle_timeout(Time, State).

-spec config_change({Out, Timeout, Drop, Max}, Time, State) ->
    {NState, TimeoutNext} when
      Out :: out | out_r,
      Timeout :: timeout(),
      Drop :: drop | drop_r,
      Max :: non_neg_integer() | infinity,
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
config_change(Arg, Time, State) ->
    NState = change(Arg, Time, State),
    handle_timeout(Time, NState).

%% @private
-spec to_list(State) -> [{SendTime, From, Value}] when
      State :: #state{},
      SendTime :: integer(),
      From :: {pid(), any()},
      Value :: any().
to_list(#state{queue=Q}) ->
    [erlang:delete_element(4, Item) || Item <- queue:to_list(Q)].

%% @private
-spec len(State) -> Len when
      State :: #state{},
      Len :: non_neg_integer().
len(#state{len=Len}) ->
    Len.

%% @private
-spec terminate(Reason, State) -> ok when
      Reason :: any(),
      State :: #state{}.
terminate(_, #state{queue=Q}) ->
    _ = [demonitor(Ref, [flush]) || {_, _, _, Ref} <- queue:to_list(Q)],
    ok.

%% Internal

first_timeout_next(_, infinity) ->
    infinity;
first_timeout_next(Time, Timeout)
  when is_integer(Timeout) andalso Timeout >= 0->
    Time.

timeout_next(_, infinity) ->
    infinity;
timeout_next(Time, Timeout) ->
    Time + Timeout.

timeout(Time, #state{timeout_next=Next} = State) when Time < Next ->
    State;
timeout(_, #state{len=0} = State) ->
    State#state{timeout_next=infinity};
timeout(Time, #state{timeout=Timeout, len=Len, queue=Q} = State) ->
    timeout(Time-Timeout, Time, Len, Q, State).

timeout(MinSend, Time, Len, Q, #state{timeout=Timeout} = State) ->
    case queue:get(Q) of
        {SendTime, _, _, _} when SendTime > MinSend ->
            State#state{timeout_next=SendTime+Timeout, len=Len, queue=Q};
        Item when Len =:= 1 ->
            drop_item(Time, Item),
            State#state{len=0, queue=queue:drop(Q), timeout_next=infinity};
        Item ->
            drop_item(Time, Item),
            timeout(MinSend, Time, Len-1, queue:drop(Q), State)
    end.

change({Out, Timeout, drop, 0}, Time, State) ->
    change({Out, Timeout, drop_r, 0}, Time, State);
change({Out, Timeout, Drop, infinity}, Time, State)
  when (Out =:= out orelse Out =:= out_r) andalso
       (Drop =:= drop orelse Drop =:= drop_r) ->
    State#state{out=Out, drop=Drop, max=infinity, timeout=Timeout,
                timeout_next=first_timeout_next(Time, Timeout)};
change({Out, Timeout, Drop, Max}, Time, #state{len=Len, queue=Q} = State)
  when (Out =:= out orelse Out =:= out_r) andalso
       (Drop =:= drop orelse Drop =:= drop_r) andalso
       (is_integer(Max) andalso Max >= 0) ->
    Next = first_timeout_next(Time, Timeout),
    NState = State#state{out=Out, drop=Drop, max=Max, timeout=Timeout,
                         timeout_next=Next},
    case Len - Max of
        DropCount when DropCount > 0 andalso Drop =:= drop ->
            {DropQ, NQ} = queue:split(DropCount, Q),
            drop_queue(Time, DropQ),
            NState#state{len=Max, queue=NQ};
        DropCount when DropCount > 0 andalso Drop =:= drop_r ->
            {NQ, DropQ} = queue:split(Max, Q),
            drop_queue(Time, DropQ),
            NState#state{len=Max, queue=NQ};
        _ ->
            NState
    end.

drop_queue(Time, Q) ->
    _ = [drop_item(Time, Item) || Item <- queue:to_list(Q)],
    ok.

drop_item(Time, {SendTime, From, _, Ref}) ->
    demonitor(Ref, [flush]),
    sbroker_queue:drop(From, SendTime, Time).
