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
-export([handle_in/4]).
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
-type internal_queue() :: queue:queue({integer(), {pid(), any()}, reference()}).
-endif.

-record(state, {out :: out | out_r,
                timeout :: pos_integer(),
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
    #state{out=Out, timeout=Timeout, drop=Drop, max=Max,
           timeout_next=timeout_next(Time, Timeout)}.

%% @private
-spec handle_in(SendTime, From, Time, State) -> NState when
      Time :: integer(),
      SendTime :: integer(),
      From :: {pid(), any()},
      State :: #state{},
      NState :: #state{}.
handle_in(SendTime, {Pid, _} = From, Time, State) ->
    case handle_timeout(Time, State) of
        #state{max=Max, len=Max, drop=drop_r} = NState ->
            sbroker_queue:drop(From, SendTime, Time),
            NState;
        #state{max=Max, len=Max, drop=drop, queue=Q} = NState ->
            {{value, Item}, NQ} = queue:out(Q),
            drop_item(Time, Item),
            Ref = monitor(process, Pid),
            NQ2 = queue:in({SendTime, From, Ref}, NQ),
            NState#state{queue=NQ2};
        #state{len=Len, queue=Q} = NState ->
            Ref = monitor(process, Pid),
            NQ = queue:in({SendTime, From, Ref}, Q),
            NState#state{len=Len+1, queue=NQ}
    end.

%% @private
-spec handle_out(Time, State) ->
    {SendTime, From, NState} | {empty, NState} when
      Time :: integer(),
      State :: #state{},
      SendTime :: integer(),
      From :: {pid(), any()},
      NState :: #state{}.
handle_out(Time, State) ->
    case handle_timeout(Time, State) of
        #state{len=0} = NState ->
            {empty, NState};
        #state{out=out, len=Len, queue=Q} = NState ->
            {{value, {_, _, Ref} = Item}, NQ} = queue:out(Q),
            demonitor(Ref, [flush]),
            setelement(3, Item, NState#state{len=Len-1, queue=NQ});
        #state{out=out_r, len=Len, queue=Q} = NState ->
            {{value, {_, _, Ref} = Item}, NQ} = queue:out_r(Q),
            demonitor(Ref, [flush]),
            setelement(3, Item, NState#state{len=Len-1, queue=NQ})
    end.

%% @private
-spec handle_timeout(Time, State) -> State when
      Time :: integer(),
      State :: #state{}.
handle_timeout(_, #state{len=0} = State) ->
    State;
handle_timeout(Time, #state{timeout_next=Next} = State) when Time < Next ->
    State;
handle_timeout(Time, #state{timeout=Timeout, len=Len, queue=Q} = State) ->
    timeout(Time-Timeout, Time, Len, Q, State).

%% @private
-spec handle_cancel(Tag, Time, State) -> {Cancelled, NState} when
      Tag :: any(),
      Time :: integer(),
      State :: #state{},
      Cancelled :: false | pos_integer(),
      NState :: #state{}.
handle_cancel(Tag, Time, State) ->
    #state{len=Len, queue=Q} = NState = handle_timeout(Time, State),
    Cancel = fun({_, {_, Tag2}, Ref}) when Tag2 =:= Tag ->
                     demonitor(Ref, [flush]),
                     false;
                (_) ->
                     true
             end,
    NQ = queue:filter(Cancel, Q),
    case queue:len(NQ) of
        Len ->
            {false, NState};
        NLen ->
            {Len - NLen, NState#state{len=NLen, queue=NQ}}
    end.

%% @private
-spec handle_info(Msg, Time, State) -> NState when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_info({'DOWN', Ref, _, _, _}, Time, State) ->
    #state{queue=Q} = NState = handle_timeout(Time, State),
    NQ = queue:filter(fun({_, _, Ref2}) -> Ref2 =/= Ref end, Q),
    NState#state{len=queue:len(NQ), queue=NQ};
handle_info(_, Time, State) ->
    handle_timeout(Time, State).

-spec config_change({Out, Timeout, Drop, Max}, Time, State) -> NState when
      Out :: out | out_r,
      Timeout :: timeout(),
      Drop :: drop | drop_r,
      Max :: non_neg_integer() | infinity,
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
config_change(Arg, Time, State) ->
    NState = change(Arg, Time, State),
    handle_timeout(Time, NState).

%% @private
-spec to_list(State) -> [{SendTime, From}] when
      State :: #state{},
      SendTime :: integer(),
      From :: {pid(), any()}.
to_list(#state{queue=Q}) ->
    [erlang:delete_element(3, Item) || Item <- queue:to_list(Q)].

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
    _ = [demonitor(Ref, [flush]) || {_, _, Ref} <- queue:to_list(Q)],
    ok.

%% Internal

timeout_next(_, infinity) ->
    infinity;
timeout_next(Time, Timeout) when is_integer(Timeout) andalso Timeout >= 0 ->
    Time.

timeout(MinSend, Time, Len, Q, #state{timeout=Timeout} = State) ->
    case queue:get(Q) of
        {SendTime, _, _} when SendTime > MinSend ->
            State#state{timeout_next=SendTime+Timeout, len=Len, queue=Q};
        Item when Len =:= 1 ->
            drop_item(Time, Item),
            State#state{len=0, queue=queue:drop(Q)};
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
                timeout_next=timeout_next(Time, Timeout)};
change({Out, Timeout, Drop, Max}, Time, #state{len=Len, queue=Q} = State)
  when (Out =:= out orelse Out =:= out_r) andalso
       (Drop =:= drop orelse Drop =:= drop_r) andalso
       (is_integer(Max) andalso Max >= 0) ->
    Next = timeout_next(Time, Timeout),
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

drop_item(Time, {SendTime, From, Ref}) ->
    demonitor(Ref, [flush]),
    sbroker_queue:drop(From, SendTime, Time).
