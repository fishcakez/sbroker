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
%% @doc Implements a head or tail drop queue with queue management based on
%% CoDel (Controlling Queue Delay).
%%
%% `sbroker_codel_queue' can be used as a `sbroker_queue' in a `sbroker' or
%% `sregulator'. It will provide a FIFO or LIFO queue that drops request in the
%% queue using CoDel when the minimum size is exceeded, and drops the head or
%% tail request from the queue when a maximum size is exceeded. Its argument,
%% `spec()', is of the form:
%% ```
%% #{out      => Out :: out | out_r, % default: out
%%   target   => Target :: non_neg_integer(), % default: 100
%%   interval => Interval :: pos_integer(), % default: 1000
%%   drop     => Drop :: drop | drop_r, % default: drop_r
%%   min      => Min :: non_neg_integer(), % default: 0
%%   max      => Max :: non_neg_integer() | infinity} % default: infinity
%% '''
%% `Out' is either `out' for a FIFO queue (the default) or `out_r' for a LIFO
%% queue. `Target' is the target queue sojourn time in milliseconds (defaults to
%% `100'). `Interval' is in the initial interval in milliseconds (defaults to
%% `1000') between drops when the queue is above the minimum size
%% `Min' (defaults to `0'). `Drop' is either `drop_r' for tail drop (the
%% default) where the last request is droppped, or `drop' for head drop, where
%% the first request is dropped. Dropping occurs when queue is above the
%% maximum size `Max' (defaults to `infinity').
%%
%% Initial parameters are recommended to be between the 95th and 99th percentile
%% round trip time for the interval and the target between 5% and 10% of the
%% interval. The round trip should be that between the actual initiator of the
%% request (e.g. a remote client) and the queue. For example, the reference
%% suggests an interval of 100ms and a target of 5ms when queuing TCP packets in
%% a kernel's buffer. A request and response might be a few more round trips at
%% the packet level even if using a single `:gen_tcp.send/2'.
%%
%% A person perceives a response time of `100' milliseconds or less as
%% instantaneous, and feels engaged with a system if response times are `1000'
%% milliseconds or less. Therefore it is desirable for a system to respond
%% within `1000' milliseconds as a worst case (upper percentile response time)
%% and ideally to respond within `100' milliseconds (target response time).
%% These also match with the suggested ratios for CoDel and so the default
%% target is `100' milliseconds and the default is interval `1000' milliseconds.
%%
%% This implementation differs from the reference as enqueue and other functions
%% can detect a slow queue and drop items. However once a slow item has been
%% detected only `handle_out/2' can detect the queue becoming fast again with
%% the caveat that with `out_r' this can only be detected if the queue reaches
%% the minimum size `Min' or less, and not a fast sojourn time. This means that
%% it is possible to drop items without calling `handle_out/2' but it is not
%% possible to stop dropping unless a `handle_out/2' dequeues an item to take
%% the queue to or below the minimum size `Min' or `out' is used and a dequeued
%% request is below target sojourn time. Therefore if `handle_out/2' is not
%% called for an extended period the queue will converge to dropping all items
%% above the target sojourn time if a single item has a sojourn time above
%% target. Whereas with the reference implementation no items would be dropped.
%%
%% If it is possible for the counterparty in the broker to "disappear" for a
%% period of time then setting a `Min' above `0' can leave `Min' items in the
%% queue for an extended period of time as requests are only dropped when the
%% queue size is above `Min'. This may be undesirable for client requests
%% because the request could wait in the queue indefinitely if there are not
%% enough requests to take the queue above `Min'. However it might be desired
%% for database connections where it is ideal for a small number of connections
%% to be waiting to handle a client request.
%%
%% @reference Kathleen Nichols and Van Jacobson, Controlling Queue Delay,
%% ACM Queue, 6th May 2012.
%% @reference Stuart Card, George Robertson and Jock Mackinlay, The Information
%% Visualizer: An Information Workspace, ACM Conference on Human Factors in
%% Computing Systems, 1991.
-module(sbroker_codel_queue).

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
-export([code_change/4]).
-export([config_change/3]).
-export([len/1]).
-export([send_time/1]).
-export([terminate/2]).

%% types

-type spec() ::
    #{out      => Out :: out | out_r,
      target   => Target :: non_neg_integer(),
      interval => Interval :: pos_integer(),
      drop     => Drop :: drop | drop_r,
      min      => Min :: non_neg_integer(),
      max      => Max :: non_neg_integer() | infinity}.

-export_type([spec/0]).

-record(state, {out :: out | out_r,
                target :: non_neg_integer(),
                interval :: pos_integer(),
                drop :: drop | drop_r,
                min :: non_neg_integer(),
                max :: non_neg_integer() | infinity,
                count=0 :: non_neg_integer(),
                drop_next :: integer(),
                drop_first=infinity :: integer() | infinity | dropping,
                timeout_next :: integer() | infinity,
                len :: non_neg_integer(),
                queue :: sbroker_queue:internal_queue()}).

%% public API

%% @private
-spec init(Q, Time, Spec) -> {State, NextTimeout} when
      Q :: sbroker_queue:internal_queue(),
      Time :: integer(),
      Spec :: spec(),
      State :: #state{},
      NextTimeout :: integer() | infinity.
init(Q, Time, Spec) ->
    handle_timeout(Time, from_queue(Q, queue:len(Q), Time, Spec)).

%% @private
-spec handle_in(SendTime, From, Value, Time, State) ->
    {NState, NextTimeout} when
      Time :: integer(),
      SendTime :: integer(),
      From :: {pid(), any()},
      Value :: any(),
      State :: #state{},
      NState :: #state{},
      NextTimeout :: integer() | infinity.
handle_in(SendTime, From, _, Time,
          #state{max=Max, len=Max, drop=drop_r} = State) ->
    sbroker_queue:drop(From, SendTime, Time),
    handle_timeout(Time, State);
handle_in(SendTime, {Pid, _} = From, Value, Time,
          #state{max=Max, len=Max, drop=drop, queue=Q} = State) ->
    {{value, Item}, NQ} = queue:out(Q),
    drop_item(Time, Item),
    Ref = monitor(process, Pid),
    NQ2 = queue:in({SendTime, From, Value, Ref}, NQ),
    in(Max, NQ2, Time, State);
handle_in(SendTime, {Pid, _} = From, Value, Time,
          #state{len=Len, queue=Q} = State) ->
    Ref = monitor(process, Pid),
    NQ = queue:in({SendTime, From, Value, Ref}, Q),
    in(Len+1, NQ, Time, State).

%% @private
-spec handle_out(Time, State) ->
    {SendTime, From, Value, Ref, NState, NextTimeout} | {empty, NState} when
      Time :: integer(),
      State :: #state{},
      SendTime :: integer(),
      From :: {pid(), any()},
      Value :: any(),
      Ref :: reference(),
      NState :: #state{},
      NextTimeout :: integer() | infinity.
handle_out(_, #state{len=0, drop_first=infinity} = State) ->
    {empty, State};
handle_out(Time, #state{len=0} = State) ->
    {empty, short(Time, State)};
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
        {empty, #state{drop_next=DropNext, interval=Interval} = NState} ->
            {empty, NState, max(DropNext + 8 * Interval, Time)}
    end.

%% @private
-spec handle_timeout(Time, State) -> {State, NextTimeout} when
      Time :: integer(),
      State :: #state{},
      NextTimeout :: integer() | infinity.
handle_timeout(_, #state{len=Len, min=Min} = State)
  when Len =< Min ->
    {State, infinity};
handle_timeout(Time, #state{timeout_next=TimeoutNext} = State)
  when TimeoutNext > Time ->
    {State, TimeoutNext};
handle_timeout(Time, #state{drop_first=dropping, drop_next=DropNext} = State)
  when DropNext > Time ->
    {State, DropNext};
handle_timeout(Time, #state{drop_first=DropFirst} = State)
  when is_integer(DropFirst) andalso DropFirst > Time ->
    {State, DropFirst};
handle_timeout(Time, #state{min=Min, len=Len, queue=Q} = State) ->
    timeout(Min, Len, Q, Time, State).

%% @private
-spec handle_cancel(Tag, Time, State) -> {Cancelled, NState, NextTimeout} when
      Tag :: any(),
      Time :: integer(),
      State :: #state{},
      Cancelled :: false | pos_integer(),
      NState :: #state{},
      NextTimeout :: integer() | infinity.
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
-spec handle_info(Msg, Time, State) -> {NState, NextTimeout} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      NextTimeout :: integer() | infinity.
handle_info({'DOWN', Ref, _, _, _}, Time, #state{queue=Q} = State) ->
    NQ = queue:filter(fun({_, _, _, Ref2}) -> Ref2 =/= Ref end, Q),
    handle_timeout(Time, State#state{len=queue:len(NQ), queue=NQ});
handle_info(_, Time, State) ->
    handle_timeout(Time, State).

%% @private
-spec code_change(OldVsn, Time, State, Extra) -> {NState, NextTimeout} when
      OldVsn :: any(),
      Time :: integer(),
      State :: #state{},
      Extra :: any(),
      NState :: #state{},
      NextTimeout :: integer() | infinity.
code_change(_, Time, State, _) ->
    {State, max(Time, timeout_next(State))}.

%% @private
-spec config_change(Spec, Time, State) -> {NState, NextTimeout} when
      Spec :: spec(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      NextTimeout :: integer() | infinity.
config_change(Spec, Time,
              #state{drop_first=DropFirst, drop_next=DropNext,
                     timeout_next=TimeoutNext, count=C, len=Len, queue=Q}) ->
    State = from_queue(Q, Len, Time, Spec),
    NState = State#state{drop_first=DropFirst, drop_next=DropNext,
                         timeout_next=TimeoutNext, count=C},
    change(Time, NState).

%% @private
-spec len(State) -> Len when
      State :: #state{},
      Len :: non_neg_integer().
len(#state{len=Len}) ->
    Len.

%% @private
-spec send_time(State) -> SendTime | empty when
      State :: #state{},
      SendTime :: integer().
send_time(#state{len=0}) ->
    empty;
send_time(#state{queue=Q}) ->
    {SendTime, _, _, _} = queue:get(Q),
    SendTime.

%% @private
-spec terminate(Reason, State) -> Q when
      Reason :: any(),
      State :: #state{},
      Q :: sbroker_queue:internal_queue().
terminate(_, #state{queue=Q}) ->
    Q.

%% Internal

in(Len, Q, Time, #state{timeout_next=TimeoutNext, min=Min} = State)
  when TimeoutNext > Time, Len > Min ->
    {State#state{len=Len, queue=Q}, TimeoutNext};
in(Len, Q, Time, #state{min=Min} = State) when Len > Min ->
    timeout(Min, Len, Q, Time, State);
in(Len, Q, _, State) ->
    {State#state{len=Len, queue=Q}, infinity}.

out({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
    #state{min=Min} = State) when Len < Min ->
    {SendTime, From, Value, Ref, short(Len, Q, Time, State), infinity};
out({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
    #state{target=Target} = State) when SendTime+Target > Time ->
    NState = fast(SendTime+Target, Len, Q, State),
    {SendTime, From, Value, Ref, NState, timeout_next(NState)};
out({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
    #state{timeout_next=TimeoutNext} = State) when TimeoutNext > Time ->
    NState = State#state{len=Len, queue=Q},
    {SendTime, From, Value, Ref, NState, timeout_next(NState)};
out({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
    #state{drop_first=infinity} = State) ->
    NState = slow(Len, Q, Time, State),
    {SendTime, From, Value, Ref, NState, timeout_next(NState)};
out({{value, Item}, Q}, Len, Time,
    #state{drop_first=dropping, target=Target, min=Min,
           drop_next=DropNext, count=C} = State) ->
    drop_item(Time, Item),
    case queue:out(Q) of
        {empty, NQ} ->
            {empty, short(0, NQ, Time, State)};
        {{value, {SendTime, From, Value, Ref}}, NQ} when Len =:= Min ->
            NState = short(Len-1, NQ, Time, State),
            {SendTime, From, Value, Ref, NState, infinity};
        {{value, {SendTime, From, Value, Ref}}, NQ}
          when SendTime+Target > Time ->
            NState = fast(SendTime+Target, Len-1, NQ, State),
            {SendTime, From, Value, Ref, NState, timeout_next(NState)};
        Out ->
            NState = drop_control(C+1, DropNext, State),
            out(Out, Len-1, Time, NState)
    end;
out({{value, Item}, Q}, Len, Time, #state{min=Min, target=Target} = State) ->
    drop_item(Time, Item),
    {Out, NQ} = queue:out(Q),
    NState = drop_control(Time, State),
    case Out of
        {value, {SendTime, From, Value, Ref}} when Len =:= Min ->
            NState2 = short(Len-1, NQ, Time, NState),
            {SendTime, From, Value, Ref, NState2, infinity};
        {value, {SendTime, From, Value, Ref}} when SendTime+Target > Time ->
            NState2 = fast(SendTime+Target, Len-1, NQ, NState),
            {SendTime, From, Value, Ref, NState2, timeout_next(NState2)};
        {value, {SendTime, From, Value, Ref}} ->
            NState2 = NState#state{len=Len-1, queue=NQ},
            {SendTime, From, Value, Ref, NState2, timeout_next(NState2)};
        empty ->
            {empty, short(0, NQ, Time, NState)}
    end.

out_r({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
    #state{min=Min} = State) when Len < Min ->
    {SendTime, From, Value, Ref, short(Len, Q, Time, State), infinity};
out_r({{value, {SendTime, From, Value, Ref}}, Q}, Len, Time,
    #state{timeout_next=TimeoutNext} = State) when TimeoutNext > Time ->
    NState = State#state{queue=Q, len=Len},
    {SendTime, From, Value, Ref, NState, timeout_next(NState)};
out_r({{value, {SendTime, From, Value, Ref}}, Q}, 0, Time,
    #state{target=Target} = State) when SendTime+Target > Time ->
    {SendTime, From, Value, Ref, short(0, Q, Time, State), infinity};
out_r({{value, Item}, Q}, 0, Time, State) ->
    out_r_empty(Item, Time, State#state{len=0, queue=Q});
out_r({{value, {SendTime, From, Value, Ref} = Item}, Q}, Len, Time,
      #state{min=Min} = State) ->
    % Removed an item already
    {NState, TimeoutNext} = timeout(max(Min-1, 0), Len, Q, Time, State),
    case NState of
        #state{len=0} when Min =:= 0 ->
            out_r_empty(Item, Time, NState);
        #state{len=NLen} when NLen < Min ->
            {SendTime, From, Value, Ref, short(Time, NState), infinity};
        #state{len=Min} ->
            {SendTime, From, Value, Ref, NState, infinity};
        _ ->
            {SendTime, From, Value, Ref, NState, TimeoutNext}
    end.

out_r_empty({SendTime, From, Value, Ref}, Time,
            #state{drop_first=infinity} = State) ->
    {SendTime, From, Value, Ref, slow(Time, State), infinity};
out_r_empty(Item, Time,
            #state{drop_first=dropping, drop_next=DropNext, count=C} = State)
          when DropNext =< Time ->
    drop_item(Time, Item),
    {empty, short(Time, drop_control(C+1, DropNext, State))};
out_r_empty(Item, Time, #state{drop_first=DropFirst} = State)
  when DropFirst =< Time ->
    drop_item(Time, Item),
    {empty, short(Time, drop_control(Time, State))};
out_r_empty({SendTime, From, Value, Ref}, _, State) ->
    {SendTime, From, Value, Ref, State, infinity}.

timeout(Min, Len, Q, Time,
        #state{target=Target, count=C, drop_first=DropFirst,
               drop_next=DropNext} = State) ->
    {SendTime, _, _, _} = Item = queue:get(Q),
    case SendTime+Target of
        TimeoutNext when TimeoutNext > Time ->
            NState = State#state{len=Len, queue=Q, timeout_next=TimeoutNext},
            {NState, TimeoutNext};
        _ when DropFirst == infinity ->
            NState = slow(Len, Q, Time, State),
            {NState, NState#state.timeout_next};
        _ when DropFirst == dropping ->
            drop_item(Time, Item),
            NState = drop_control(C+1, DropNext, State),
            drop_next(Min, Len-1, queue:drop(Q), Time, NState);
        _ ->
            drop_item(Time, Item),
            NQ = queue:drop(Q),
            NState = drop_control(Time, State#state{len=Len-1, queue=NQ}),
            {NState, timeout_next(NState)}
    end.

timeout_next(#state{len=Len, min=Min, timeout_next=TimeoutNext})
  when Len > Min ->
    TimeoutNext;
timeout_next(_) ->
    infinity.

drop_next(Min, Len, Q, Time, #state{drop_next=DropNext} = State)
  when DropNext > Time, Len > Min ->
    {State#state{len=Len, queue=Q}, DropNext};
drop_next(Min, Len, Q, Time, #state{count=C, drop_next=DropNext} = State)
  when Len > Min ->
    {{value, Item}, NQ} = queue:out(Q),
    drop_item(Time, Item),
    NState = drop_control(C+1, DropNext, State),
    drop_next(Min, Len-1, NQ, Time, NState);
drop_next(_, Len, Q, _, State) ->
    {State#state{len=Len, queue=Q}, infinity}.

short(Time, State) ->
    State#state{drop_first=infinity, timeout_next=Time}.

short(Len, Q, Time, State) ->
    State#state{len=Len, queue=Q, drop_first=infinity, timeout_next=Time}.

fast(TimeoutNext, Len, Q, State) ->
    State#state{len=Len, queue=Q, drop_first=infinity,
                timeout_next=TimeoutNext}.

slow(Time, #state{interval=Interval} = State) ->
    DropFirst=Time+Interval,
    State#state{timeout_next=DropFirst, drop_first=DropFirst}.

slow(Len, Q, Time, #state{interval=Interval} = State) ->
    DropFirst=Time+Interval,
    State#state{len=Len, queue=Q, timeout_next=DropFirst, drop_first=DropFirst}.

%% If first "slow" item in "slow" interval was "soon" after switching from
%% dropping to not dropping use the previous dropping interval length as it
%% should be appropriate - as done in CoDel draft implemenation.
drop_control(Time, #state{interval=Interval, count=C,
                          drop_next=DropNext} = State)
  when C > 2 andalso Time - DropNext < 8 * Interval ->
    drop_control(C - 2, Time, State#state{drop_first=dropping});
drop_control(Time, #state{interval=Interval} = State) ->
    DropNext = Time+Interval,
    State#state{count=1, timeout_next=DropNext, drop_next=DropNext,
                drop_first=dropping}.

%% Shrink the interval to increase drop rate and reduce sojourn time.
drop_control(C, Time, #state{interval=Interval} = State) ->
    DropNext = Time + trunc(Interval / math:sqrt(C)),
    State#state{count=C, timeout_next=DropNext, drop_next=DropNext}.

drop_item(Time, {SendTime, From, _, Ref}) ->
    demonitor(Ref, [flush]),
    sbroker_queue:drop(From, SendTime, Time).

drop_queue(Time, Q) ->
   Drop = fun({SendTime, From, _, Ref}) ->
                   demonitor(Ref, [flush]),
                   sbroker_queue:drop(From, SendTime, Time),
                   false
           end,
    _ = queue:filter(Drop, Q),
    ok.

from_queue(Q, Len, Time, Spec) ->
    Out = sbroker_util:out(Spec),
    Interval = sbroker_util:interval(Spec),
    Target = sbroker_util:sojourn_target(Spec),
    Drop = sbroker_util:drop(Spec),
    {Min, Max} = sbroker_util:min_max(Spec),
    State = #state{out=Out, target=Target, interval=Interval, drop=Drop,
                   min=Min, max=Max, drop_next=Time, timeout_next=Time, len=Len,
                   queue=Q},
    if
        Len > Max andalso Drop =:= drop ->
            {DropQ, NQ} = queue:split(Len-Max, Q),
            drop_queue(Time, DropQ),
            State#state{len=Max, queue=NQ};
        Len > Max andalso Drop =:= drop_r ->
            {NQ, DropQ} = queue:split(Max, Q),
            drop_queue(Time, DropQ),
            State#state{len=Max, queue=NQ};
        true ->
            State
    end.

change(Time, #state{drop_first=DropFirst, interval=Interval} = State)
  when is_integer(DropFirst) andalso DropFirst > Time+Interval ->
    NDropFirst = Time+Interval,
    change(Time, State#state{drop_first=NDropFirst, timeout_next=NDropFirst});
change(Time,
       #state{drop_next=DropNext, interval=Interval,
              drop_first=DropFirst} = State)
  when is_integer(DropNext) andalso DropNext > Time+Interval ->
    NDropNext = Time+Interval,
    case DropFirst of
        dropping ->
            NState = State#state{drop_next=NDropNext, timeout_next=NDropNext},
            change(Time, NState);
        _ ->
            change(Time, State#state{drop_next=NDropNext})
    end;
change(Time, #state{drop_first=infinity} = State) ->
    handle_timeout(Time, State#state{timeout_next=Time});
change(Time, State) ->
    handle_timeout(Time, State).
