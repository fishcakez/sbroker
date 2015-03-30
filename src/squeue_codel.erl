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
%% @doc Implements CoDel based roughly on Controlling Queue Delay, see
%% reference.
%%
%% `squeue_codel' can be used as the active queue management module in a
%% `squeue' queue. It's arguments are of the form `{Target, Interval}', with
%% `Target', `non_neg_integer()', the target sojourn time of an item in the
%% queue and `Interval', `pos_integer()', the initial interval between drops
%% once the queue becomes slow.
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
-module(squeue_codel).

-behaviour(squeue).

-export([init/2]).
-export([handle_timeout/3]).
-export([handle_out/3]).
-export([handle_out_r/3]).
-export([handle_join/3]).

-record(state, {target :: non_neg_integer(),
                interval :: pos_integer(),
                count=0 :: non_neg_integer(),
                drop_next :: integer(),
                drop_first=infinity :: integer() | infinity | dropping,
                peek_next :: integer()}).

-opaque state() :: #state{}.

-export_type([state/0]).

%% @private
-spec init(Time, {Target, Interval}) -> State when
      Time :: integer(),
      Target :: non_neg_integer(),
      Interval :: pos_integer(),
      State :: state().
init(Time, {Target, Interval})
  when is_integer(Target) andalso Target >= 0 andalso
       is_integer(Interval) andalso Interval > 0 ->
    #state{target=Target, interval=Interval, drop_next=Time, peek_next=Time}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_timeout(Time, Q, State) -> {[], Q, State} when
      Time :: integer(),
      Q :: queue(),
      State :: state().
-else.
-spec handle_timeout(Time, Q, State) -> {[], Q, State} when
      Time :: integer(),
      Q :: queue:queue(),
      State :: state().
-endif.
handle_timeout(Time, Q, #state{peek_next=PeekNext} = State)
  when PeekNext > Time->
    {[], Q, State};
handle_timeout(Time, Q, #state{drop_first=dropping, drop_next=DropNext} = State)
  when DropNext > Time ->
    {[], Q, State};
handle_timeout(Time, Q, #state{drop_first=DropFirst} = State)
  when is_integer(DropFirst) andalso DropFirst > Time ->
    {[], Q, State};
handle_timeout(Time, Q, #state{target=Target} = State) ->
    timeout_peek(queue:peek(Q), Time - Target, Time, Q, State).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_out(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue(),
      State :: state(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: state().
-else.
-spec handle_out(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue:queue(Item),
      State :: state(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: state().
-endif.
handle_out(Time, Q, #state{peek_next=PeekNext} = State) when Time < PeekNext ->
    {[], Q, State};
handle_out(Time, Q, #state{target=Target} = State) ->
    out_peek(queue:peek(Q), Time - Target, Time, Q, State).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_out_r(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue(),
      State :: state(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: state().
-else.
-spec handle_out_r(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue:queue(Item),
      State :: state(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: state().
-endif.
handle_out_r(Time, Q, State) ->
    case handle_timeout(Time, Q, State) of
        {_, _, #state{drop_first=infinity}} = Result ->
            Result;
        {Drops, NQ, NState} ->
            {Drops, NQ, out_r_peek(Time, NQ, NState)}
    end.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_join(Time, Q, State) -> {[], Q, NState} when
      Time :: integer(),
      Q :: queue(),
      State :: state(),
      NState :: state().
-else.
-spec handle_join(Time, Q, State) -> {[], Q, NState} when
      Time :: integer(),
      Q :: queue:queue(),
      State :: state(),
      NState :: state().
-endif.
handle_join(Time, Q, State) ->
    case queue:is_empty(Q) of
        true ->
            {[], Q, State#state{peek_next=Time}};
        false ->
            {[], Q, State}
    end.

%% Internal

timeout_peek(empty, _MinStart, _Time, Q, #state{drop_first=infinity} = State) ->
    {[], Q, State};
timeout_peek(empty, _MinStart, _Time, Q, State) ->
    {[], Q, State};
timeout_peek({value, {Start, _}}, MinStart, _Time, Q,
             #state{drop_first=infinity, target=Target} = State)
  when Start > MinStart ->
    {[], Q, State#state{peek_next=Start+Target}};
timeout_peek({value, {Start, _}}, MinStart, _Time, Q, State)
  when Start > MinStart ->
   {[], Q, State};
timeout_peek(_Result, _MinStart, Time, Q,
             #state{drop_first=infinity, interval=Interval} = State) ->
    {[], Q, State#state{drop_first=Time+Interval}};
%% Queue is slow and next/first drop is due.
timeout_peek({value, Item}, MinStart, Time, Q,
             #state{drop_first=dropping, count=C,
                    drop_next=DropNext} = State) ->
    NQ = queue:drop(Q),
    case drop_control(C+1, DropNext, State) of
        #state{drop_next=NDropNext} = NState when NDropNext > Time ->
            {[Item], NQ, NState};
        NState ->
            timeout_drops(queue:peek(NQ), MinStart, Time, NQ, NState, [Item])
    end;
timeout_peek({value, Item}, _MinStart, Time, Q,
             #state{drop_first=DropFirst} = State) when is_integer(DropFirst) ->
    NQ = queue:drop(Q),
    NState = drop_control(Time, State),
    {[Item], NQ, NState}.

timeout_drops(empty, _MinStart, _Time, Q, State, Drops) ->
    {Drops, Q, State};
timeout_drops({value, {Start, _}}, MinStart, _Time, Q, State, Drops)
  when Start > MinStart ->
    {Drops, Q, State};
timeout_drops({value, Item}, MinStart, Time, Q,
      #state{count=C, drop_next=DropNext} = State, Drops) ->
    NQ = queue:drop(Q),
    case drop_control(C+1, DropNext, State) of
        #state{drop_next=NDropNext} = NState when NDropNext > Time ->
            {[Item | Drops], NQ, NState};
        NState ->
            NDrops = [Item | Drops],
            timeout_drops(queue:peek(NQ), MinStart, Time, NQ, NState, NDrops)
    end.

%% Empty queue so reset drop_first
out_peek(empty,  _MinStart, _Time, Q, State) ->
    %% The tail time is unknown so the sojourn time of the next item could be
    %% above target.
    {[], Q, State#state{drop_first=infinity}};
%% Item below target sojourn time and getting dequeued
out_peek({value, {Start, _}}, MinStart, _Time, Q, #state{target=Target} = State)
  when Start > MinStart ->
    %% First time state can change is if the next item has the same start time
    %% and remains for the target sojourn time.
    {[], Q, State#state{drop_first=infinity, peek_next=Start+Target}};
%% Item is first above target sojourn time, begin first interval.
out_peek(_Result, _MinStart, Time, Q,
         #state{drop_first=infinity, interval=Interval} = State) ->
    {[], Q, State#state{drop_first=Time + Interval}};
%% Item above target sojourn time during a consecutive "slow" interval.
out_peek(_Result, _MinStart, Time, Q,
          #state{drop_first=dropping, drop_next=DropNext} = State)
  when DropNext > Time ->
    {[], Q, State};
%% Item above target sojourn time and is the last in a consecutive "slow"
%% interval.
out_peek({value, Item}, MinStart, Time, Q,
     #state{drop_first=dropping} = State) ->
    NQ = queue:drop(Q),
    out_drops(queue:peek(NQ), MinStart, Time, NQ, State, [Item]);
%% Item above target sojourn time during the first "slow" interval.
out_peek(_Result, _MinStart, Time, Q, #state{drop_first=DropFirst} = State)
  when DropFirst > Time ->
    {[], Q, State};
%% Item above target sojourn time and is the last item in the first "slow"
%% interval so drop it.
out_peek({value, Item}, MinStart, Time, Q, State) ->
    NQ = queue:drop(Q),
    NState = drop_control(Time, State),
    case queue:peek(NQ) of
        empty ->
            {[Item], NQ, NState#state{drop_first=infinity}};
        {value, {Start, _}} when Start > MinStart ->
            {[Item], NQ, NState#state{drop_first=infinity}};
        _ ->
            {[Item], NQ, NState}
    end.

out_drops(empty, _MinStart, _Time, Q, State, Drops) ->
    {Drops, Q, State#state{drop_first=infinity}};
out_drops({value, {Start, _}}, MinStart, _Time, Q,
      #state{target=Target} = State, Drops) when Start > MinStart ->
    {Drops, Q, State#state{drop_first=infinity, peek_next=Start+Target}};
out_drops({value, {Start, _}}, MinStart, _Time, Q,
          #state{count=C, drop_next=DropNext} = State, Drops)
  when Start > MinStart ->
    NState = drop_control(C+1, DropNext, State),
    {Drops, Q, NState};
out_drops({value, Item}, MinStart, Time, Q,
      #state{count=C, drop_next=DropNext} = State, Drops) ->
    case drop_control(C+1, DropNext, State) of
        #state{drop_next=NDropNext} = NState when NDropNext > Time ->
            {Drops, Q, NState};
        NState ->
            NQ = queue:drop(Q),
            NDrops = [Item | Drops],
            out_drops(queue:peek(NQ), MinStart, Time, NQ, NState, NDrops)
    end.

out_r_peek(_Time, Q, State) ->
    case queue:is_empty(Q) of
        true ->
            State#state{drop_first=infinity};
        false ->
            State
    end.

%% If first "slow" item in "slow" interval was "soon" after switching from
%% dropping to not dropping use the previous dropping interval length as it
%% should be appropriate - as done in CoDel draft implemenation.
drop_control(Time, #state{interval=Interval, count=C,
                          drop_next=DropNext} = State)
  when C > 2 andalso Time - DropNext < Interval ->
    drop_control(C - 2, Time, State);
drop_control(Time, State) ->
    drop_control(1, Time, State).

%% Shrink the interval to increase drop rate and reduce sojourn time.
drop_control(C, Time, #state{interval=Interval} = State) ->
    DropNext = Time + trunc(Interval / math:sqrt(C)),
    State#state{count=C, drop_next=DropNext, drop_first=dropping}.
