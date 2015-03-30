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
%% @doc Implements a feedback loop similar to CoDel, see reference, but tries to
%% keep the sojourn time of another queue above a target by dequeuing items from
%% the queue when the other queue is below target.
%%
%% `svalve_codel' can be used as the feedback loop module in a `svalve' queue.
%% It's argument is of the form `{Target, Interval}', with `Target',
%% `non_neg_integer()', the target sojourn time of an item in another queue and
%% `Interval', `non_neg_integer()', the initial interval between dequeues when
%% the other queue is fast.
%%
%% @reference Kathleen Nichols and Van Jacobson, Controlling Queue Delay,
%% ACM Queue, 6th May 2012.
-module(svalve_codel_r).

-behaviour(svalve).

-export([init/2]).
-export([handle_sojourn/4]).
-export([handle_sojourn_r/4]).
-export([handle_sojourn_closed/4]).
-export([handle_dropped/3]).
-export([handle_dropped_r/3]).
-export([handle_dropped_closed/3]).
-record(state, {target :: non_neg_integer(),
                interval :: non_neg_integer(),
                count=0 :: non_neg_integer(),
                dequeue_next :: integer(),
                dequeue_first=infinity :: integer() | infinity | dequeuing}).

%% @private
-spec init(Time, {Target, Interval}) -> State when
      Time :: integer(),
      Target :: non_neg_integer(),
      Interval :: non_neg_integer(),
      State :: #state{}.
init(Time, {Target, Interval}) ->
    #state{target=Target, interval=Interval, dequeue_next=Time}.

%% @private
-spec handle_sojourn(Time, SojournTime, S, State) ->
    {Result, Drops, NS, NState} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      S :: squeue:squeue(Item),
      State :: #state{},
      Result :: closed | empty | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue:squeue(Item),
      NState :: #state{}.
handle_sojourn(Time, SojournTime, S, State) ->
    handle(Time, SojournTime, S, out, State).

%% @private
-spec handle_sojourn_r(Time, SojournTime, S, State) ->
    {Result, Drops, NS, NState} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      S :: squeue:squeue(Item),
      State :: #state{},
      Result :: closed | empty | {ItemSojournTime :: non_neg_integer(), Item},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue:squeue(Item),
      NState :: #state{}.
handle_sojourn_r(Time, SojournTime, S, State) ->
    handle(Time, SojournTime, S, out_r, State).

%% @private
-spec handle_sojourn_closed(Time, SojournTime, S, State) ->
    {closed, Drops, NS, NState} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      S :: squeue:squeue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue:squeue(Item),
      NState :: #state{}.
handle_sojourn_closed(Time, SojournTime, S, State) ->
    handle(Time, SojournTime, S, timeout, State).

%% @private
-spec handle_dropped(Time, S, State) ->
    {closed, Drops, NS, NState} when
      Time :: integer(),
      S :: squeue:squeue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue:squeue(Item),
      NState :: #state{}.
handle_dropped(Time, S, State) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, dropped_control(State)}.

%% @private
-spec handle_dropped_r(Time, S, State) ->
    {closed, Drops, NS, NState} when
      Time :: integer(),
      S :: squeue:squeue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue:squeue(Item),
      NState :: #state{}.
handle_dropped_r(Time, S, State) ->
    handle_dropped(Time, S, State).

%% @private
-spec handle_dropped_closed(Time, S, State) ->
    {closed, Drops, NS, NState} when
      Time :: integer(),
      S :: squeue:squeue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NS :: squeue:squeue(Item),
      NState :: #state{}.
handle_dropped_closed(Time, S, State) ->
    handle_dropped(Time, S, State).

%% Internal

handle(Time, SojournTime, S, _, #state{target=Target} = State)
  when SojournTime > Target ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, State#state{dequeue_first=infinity}};
handle(Time, _, S, _,
       #state{dequeue_first=infinity, interval=Interval} = State) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, State#state{dequeue_first=Time+Interval}};
handle(Time, _, S, _, #state{dequeue_first=dequeuing,
                             dequeue_next=DequeueNext} = State)
  when DequeueNext > Time ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, State};
handle(Time, _, S, timeout, #state{dequeue_first=dequeuing} = State) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, State};
handle(Time, _, S, Out, #state{dequeue_first=dequeuing, count=C,
                               dequeue_next=DequeueNext} = State) ->
    case squeue:Out(Time, S) of
        {empty, Drops, NS} ->
            {empty, Drops, NS, State};
        {Result, Drops, NS} ->
            {Result, Drops, NS, dequeue_control(C+1, DequeueNext, State)}
    end;
handle(Time, _, S, _, #state{dequeue_first=DequeueFirst} = State)
  when DequeueFirst > Time ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, State};
handle(Time, _, S, timeout, State) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, State};
handle(Time, _, S, Out, State) ->
    case squeue:Out(Time, S) of
        {empty, Drops, NS} ->
            {empty, Drops, NS, State};
        {Result, Drops, NS} ->
            {Result, Drops, NS, dequeue_control(Time, State)}
    end.

dequeue_control(Time, #state{interval=Interval, count=C,
                             dequeue_next=DequeueNext} = State)
  when C > 2 andalso Time - DequeueNext < Interval ->
    dequeue_control(C - 2, Time, State);
dequeue_control(Time, State) ->
    dequeue_control(1, Time, State).

dequeue_control(C, Time, #state{interval=Interval} = State) ->
    DequeueNext = Time + trunc(Interval / math:sqrt(C)),
    State#state{count=C, dequeue_next=DequeueNext, dequeue_first=dequeuing}.

dropped_control(#state{count=C} = State) ->
    State#state{count=max(0, C-1), dequeue_first=infinity}.
