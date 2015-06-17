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
%% @doc Implements a head or tail drop queue with queue management based roughly
%% on CoDel (Controlling Queue Delay), see reference.
%%
%% `sbroker_codel_queue' can be used as an `sbroker_queue' module in `sbroker'.
%% Its argument is of the form:
%% ```
%% {out | out_r, Target :: non_neg_integer(), Interval :: pos_integer(),
%% drop | drop_r, Max :: non_neg_integer() | infinity}.
%% '''
%% The first element is `out' for a FIFO queue and `out_r' for a LIFO queue. The
%% second element is the target queue sojourn time and the third element in the
%% initial interval between drops. The fourth element determines whether to
%% drop from head (`drop') or drop from the tail (`drop_r') when the queue is
%% above the maximum size (fifth element).
%%
%% Initial parameters are recommended to be between the 95th and 99th percentile
%% round trip time and the target between 5% and 10% of the interval. The round
%% trip should be that between the actual initiator of the request (e.g. a
%% remote client) and the queue. For example, the reference suggests an interval
%% of 100ms and a target of 5ms when queuing TCP packets in a kernel's buffer.
%%
%% This implementation differs from the reference as enqueue and other functions
%% can detect a slow queue and drop items. However once a slow item has been
%% detected only `out' can detect the queue becoming fast again - unless `out_r'
%% is used on an empty queue. This means that it is possible to drop items
%% without calling `out' but it is not possible to stop dropping unless an `out'
%% dequeues an item below target sojourn time or a dequeue attempt is
%% made on an empty queue. Therefore if `out' is not called for an extended
%% period the queue will converge to dropping all items above the target sojourn
%% time (once a single item has a sojourn time above target). Whereas with the
%% reference implementation no items would be dropped.
%%
%% @reference Kathleen Nichols and Van Jacobson, Controlling Queue Delay,
%% ACM Queue, 6th May 2012.
-module(sbroker_codel_queue).

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
                target :: non_neg_integer(),
                interval :: pos_integer(),
                drop :: drop | drop_r,
                max :: non_neg_integer() | infinity,
                count=0 :: non_neg_integer(),
                drop_next :: integer(),
                drop_first=infinity :: integer() | infinity | dropping,
                peek_next :: integer(),
                len = 0 :: non_neg_integer(),
                queue = queue:new() :: internal_queue()}).

%% public API

%% @private
-spec init(Time, {Out, Target, Interval, Drop, Max}) -> State when
      Time :: integer(),
      Out :: out | out_r,
      Target :: non_neg_integer(),
      Interval :: pos_integer(),
      Drop :: drop | drop_r,
      Max :: non_neg_integer() | infinity,
      State :: #state{}.
init(Time, {Out, Target, Interval, drop, 0}) ->
    init(Time, {Out, Target, Interval, drop_r, 0});
init(Time, {Out, Target, Interval, Drop, Max})
  when (Out =:= out orelse Out =:= out_r) andalso
       is_integer(Target) andalso Target >= 0 andalso
       is_integer(Interval) andalso Interval > 0 andalso
       (Drop =:= drop orelse Drop =:= drop_r) andalso
       ((is_integer(Max) andalso Max >= 0) orelse Max =:= infinity) ->
    #state{out=Out, target=Target, interval=Interval, drop=Drop, max=Max,
           drop_next=Time, peek_next=Time}.

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
handle_out(_, #state{len=0, drop_first=infinity} = State) ->
    {empty, State};
handle_out(_, #state{len=0} = State) ->
    {empty, State#state{drop_first=infinity}};
handle_out(Time, #state{out=out, peek_next=PeekNext, len=Len, queue=Q} = State)
  when PeekNext > Time ->
    {{value, {_, _, Ref} = Item}, NQ} = queue:out(Q),
    demonitor(Ref, [flush]),
    setelement(3, Item, State#state{len=Len-1, queue=NQ});
handle_out(Time, #state{out=out, target=Target, len=Len, queue=Q} = State) ->
    MinSend = Time - Target,
    out_peek(queue:out(Q), MinSend, Time, Len, State);
handle_out(Time, #state{out=out_r} = State) ->
    case handle_timeout(Time, State) of
        #state{len=0, drop_first=infinity} = NState ->
            {empty, NState};
        #state{len=0} = NState ->
            {empty, NState#state{drop_first=infinity}};
        #state{len=Len, queue=Q} = NState ->
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
handle_timeout(Time, #state{peek_next=PeekNext} = State) when PeekNext > Time ->
    State;
handle_timeout(Time, #state{drop_first=dropping, drop_next=DropNext} = State)
  when DropNext > Time ->
    State;
handle_timeout(Time,  #state{drop_first=DropFirst} = State)
  when is_integer(DropFirst) andalso DropFirst > Time ->
    State;
handle_timeout(Time, #state{target=Target, len=Len, queue=Q} = State) ->
    timeout_peek(queue:get(Q), Time - Target, Time, Len, Q, State).

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

-spec config_change({Out, Target, Interval, Drop, Max}, Time, State) ->
    NState when
      Out :: out | out_r,
      Target :: non_neg_integer(),
      Interval :: pos_integer(),
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


timeout_peek({Send, _, _}, MinSend, _, _, _,
             #state{drop_first=infinity, target=Target} = State)
  when Send > MinSend ->
    State#state{peek_next=Send+Target};
timeout_peek({Send, _, _}, MinSend, _, _, _, State) when Send > MinSend ->
    State;
timeout_peek(_, _, Time, _, _,
             #state{drop_first=infinity, interval=Interval} = State) ->
    State#state{drop_first=Time+Interval};
timeout_peek(Item, MinSend, Time, Len, Q,
             #state{drop_first=dropping, count=C,
                    drop_next=DropNext} = State) ->
    drop_item(Time, Item),
    NState = drop_control(C+1, DropNext, State),
    timeout_drop(MinSend, Time, Len-1, queue:drop(Q), NState);
timeout_peek(Item, _, Time, Len, Q, #state{drop_first=DropFirst} = State)
  when is_integer(DropFirst) ->
    drop_item(Time, Item),
    drop_control(Time, State#state{len=Len-1, queue=queue:drop(Q)}).

timeout_drop(_, _, 0, Q, State) ->
    State#state{len=0, queue=Q};
timeout_drop(_, Time, Len, Q, #state{drop_next=DropNext} = State)
  when DropNext > Time ->
    State#state{drop_next=DropNext, len=Len, queue=Q};
timeout_drop(MinSend, Time, Len, Q,
             #state{count=C, drop_next=DropNext} = State) ->
    case queue:get(Q) of
        {Send, _, _}  when Send > MinSend ->
            State#state{len=Len, queue=Q};
        Item ->
            drop_item(Time, Item),
            NState = drop_control(C+1, DropNext, State),
            timeout_drop(MinSend, Time, Len-1, queue:drop(Q), NState)
    end.


%% Item below target sojourn time and getting dequeued
out_peek({{value, {Send, _, Ref} = Item}, Q}, MinSend, _Time, Len,
         #state{target=Target} = State) when Send > MinSend ->
    demonitor(Ref, [flush]),
    %% First time state can change is if the next item has the same start time
    %% and remains for the target sojourn time.
    NState = State#state{drop_first=infinity, peek_next=Send+Target, len=Len-1,
                         queue=Q},
    setelement(3, Item, NState);
%% Item is first above target sojourn time, begin first interval.
out_peek({{value, {_, _, Ref} = Item}, Q}, _MinSend, Time, Len,
         #state{drop_first=infinity, interval=Interval} = State) ->
    demonitor(Ref, [flush]),
    NState = State#state{drop_first=Time+Interval, len=Len-1, queue=Q},
    setelement(3, Item, NState);
%% Item above target sojourn time during a consecutive "slow" interval.
out_peek({{value, {_, _, Ref} = Item}, Q}, _, Time, Len,
          #state{drop_first=dropping, drop_next=DropNext} = State)
  when DropNext > Time ->
    demonitor(Ref, [flush]),
    setelement(3, Item, State#state{len=Len-1, queue=Q});
%% Item above target sojourn time and is the last in a consecutive "slow"
%% interval.
out_peek({{value, Item}, Q}, _, Time, 1, #state{drop_first=dropping} = State) ->
    drop_item(Time, Item),
    {empty, State#state{drop_first=infinity, len=0, queue=Q}};
out_peek({{value, Item}, Q}, MinSend, Time, Len,
         #state{drop_first=dropping} = State) ->
    drop_item(Time, Item),
    out_drops(queue:out(Q), MinSend, Time, Len-1, State);
%% Item above target sojourn time during the first "slow" interval.
out_peek({{value, {_, _, Ref} = Item}, Q}, _, Time, Len,
         #state{drop_first=DropFirst} = State) when DropFirst > Time ->
    demonitor(Ref, [flush]),
    setelement(3, Item, State#state{len=Len-1, queue=Q});
%% Item above target sojourn time and is the last item in the first "slow"
%% interval so drop it.
out_peek({{value, Item}, Q}, _, Time, 1, State) ->
    drop_item(Time, Item),
    NState = drop_control(Time, State),
    {empty, NState#state{drop_first=infinity, len=0, queue=Q}};
out_peek({{value, Item}, Q}, MinSend, Time, Len, State) ->
    drop_item(Time, Item),
    NState = drop_control(Time, State),
    case queue:out(Q) of
        {{value, {Send, _, Ref} = Item2}, NQ} when Send > MinSend ->
            demonitor(Ref, [flush]),
            NState2 = NState#state{drop_first=infinity, len=Len-2, queue=NQ},
            setelement(3, Item2, NState2);
        {{value, {_, _, Ref} = Item2}, NQ} ->
            demonitor(Ref, [flush]),
            setelement(3, Item2, NState#state{len=Len-2, queue=NQ})
    end.

out_drops({{value, {Send, _, Ref} = Item}, Q}, MinSend, _Time, Len,
      #state{target=Target} = State) when Send > MinSend ->
    demonitor(Ref, [flush]),
    NState = State#state{drop_first=infinity, peek_next=Send+Target,
                         queue=Q, len=Len-1},
    setelement(3, Item, NState);
out_drops({{value, {_, _, Ref} = Item}, Q}, MinStart, Time, Len,
      #state{count=C, drop_next=DropNext} = State) ->
    case drop_control(C+1, DropNext, State) of
        #state{drop_next=NDropNext} = NState when NDropNext > Time ->
            demonitor(Ref, [flush]),
            setelement(3, Item, NState#state{queue=Q, len=Len-1});
        NState when Len =:= 1 ->
            drop_item(Time, Item),
            {empty, NState#state{drop_first=infinity, len=0, queue=Q}};
        NState ->
            drop_item(Time, Item),
            out_drops(queue:out(Q), MinStart, Time, Len-1, NState)
    end.

%% If first "slow" item in "slow" interval was "soon" after switching from
%% dropping to not dropping use the previous dropping interval length as it
%% should be appropriate - as done in CoDel draft implemenation.
drop_control(Time, #state{interval=Interval, count=C,
                          drop_next=DropNext} = State)
  when C > 2 andalso Time - DropNext < Interval ->
    drop_control(C - 2, Time, State#state{drop_first=dropping});
drop_control(Time, #state{interval=Interval} = State) ->
    State#state{count=1, drop_next=Time+Interval, drop_first=dropping}.

%% Shrink the interval to increase drop rate and reduce sojourn time.
drop_control(C, Time, #state{interval=Interval} = State) ->
    DropNext = Time + trunc(Interval / math:sqrt(C)),
    State#state{count=C, drop_next=DropNext}.

drop_item(Time, {SendTime, From, Ref}) ->
    demonitor(Ref, [flush]),
    sbroker_queue:drop(From, SendTime, Time).

change({Out, Target, Interval, drop, 0}, Time, State) ->
    change({Out, Target, Interval, drop_r, 0}, Time, State);
change({Out, Target, Interval, Drop, Max}, Time,
       #state{len=Len, queue=Q} = State)
  when (Out =:= out orelse Out =:= out_r) andalso
       (Drop =:= drop orelse Drop =:= drop_r) andalso
       is_integer(Target) andalso Target >= 0 andalso
       is_integer(Interval) andalso Interval > 0 andalso
       ((is_integer(Max) andalso Max >= 0) orelse Max =:= infinity) ->
    NState = State#state{out=Out, target=Target, interval=Interval,
                         drop=Drop, max=Max, peek_next=Time},
    if
        Len > Max andalso Drop =:= drop ->
            {DropQ, NQ} = queue:split(Len-Max, Q),
            drop_queue(Time, DropQ),
            change(Time, NState#state{len=Max, queue=NQ});
        Len > Max andalso Drop =:= drop_r ->
            {NQ, DropQ} = queue:split(Max, Q),
            drop_queue(Time, DropQ),
            change(Time, NState#state{len=Max, queue=NQ});
        true ->
            change(Time, NState)
    end.

change(Time, #state{drop_first=DropFirst, interval=Interval} = State)
  when is_integer(DropFirst) andalso DropFirst > Time+Interval ->
    change(Time, State#state{drop_first=Time+Interval});
change(Time, #state{drop_next=DropNext, interval=Interval} = State)
  when is_integer(DropNext) andalso DropNext > Time+Interval ->
    change(Time, State#state{drop_next=Time+Interval});
change(_, State) ->
    State.

drop_queue(Time, Q) ->
    _ = [drop_item(Time, Item) || Item <- queue:to_list(Q)],
    ok.
