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
%% reference, with the additional that items over a timeout sojourn time
%% are dequeued using CoDel.
%%
%% `squeue_codel' can be used as the active queue management module in a
%% `squeue' queue. It's arguments are of the form `{Target, Interval, Timeout}',
%% with `Target', `non_neg_integer()', the target sojourn time of an item in the
%% queue; `Interval', `pos_integer()', the initial interval between drops once
%% the queue becomes slow; `Timeout', `pos_integer()', the timeout value, i.e.
%% the minimum sojourn time at which items are dropped from the queue due to a
%% timeout. `Timeout' must be greater than `Target'.
%%
%% @reference Kathleen Nichols and Van Jacobson, Controlling Queue Delay,
%% ACM Queue, 6th May 2012.
-module(squeue_codel_timeout).

-behaviour(squeue).

-export([init/2]).
-export([handle_timeout/3]).
-export([handle_out/3]).
-export([handle_out_r/3]).
-export([handle_join/3]).

-record(state, {timeout :: pos_integer(),
                codel :: squeue_codel:state(),
                timeout_next :: integer()}).

%% @private
-spec init(Time, {Target, Interval, Timeout}) -> State when
      Time :: integer(),
      Target :: non_neg_integer(),
      Interval :: pos_integer(),
      Timeout :: pos_integer(),
      State :: #state{}.
init(Time, {Target, Interval, Timeout}) when Target < Timeout ->
    #state{timeout=Timeout, codel=squeue_codel:init(Time, {Target, Interval}),
           timeout_next=Time}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_timeout(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: #state{}.
-else.
-spec handle_timeout(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue:queue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: #state{}.
-endif.
handle_timeout(Time, Q, State) ->
   handle(Time, handle_timeout, Q, State).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_out(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: #state{}.
-else.
-spec handle_out(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue:queue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: #state{}.
-endif.
handle_out(Time, Q, State) ->
    handle(Time, handle_out, Q, State).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_out_r(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue(),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue(),
      NState :: #state{}.
-else.
-spec handle_out_r(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: queue:queue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: queue:queue(Item),
      NState :: #state{}.
-endif.
handle_out_r(Time, Q, State) ->
    handle(Time, handle_out_r, Q, State).

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_join(Time, Q, State) -> {[], Q, NState} when
      Time :: integer(),
      Q :: queue(),
      State :: #state{},
      NState :: #state{}.
-else.
-spec handle_join(Time, Q, State) -> {[], Q, NState} when
      Time :: integer(),
      Q :: queue:queue(),
      State :: #state{},
      NState :: #state{}.
-endif.
handle_join(Time, Q, #state{codel=Codel} = State) ->
    {[], NQ, NCodel} = squeue_codel:handle_join(Time, Q, Codel),
    case queue:is_empty(NQ) of
        true ->
            {[], NQ, State#state{codel=NCodel, timeout_next=Time}};
        false ->
            {[], NQ, State#state{codel=NCodel}}
    end.

%% Internal

handle(Time, Fun, Q, #state{timeout_next=TimeoutNext, codel=Codel} = State)
  when Time < TimeoutNext ->
    {Drops, NQ, NCodel} = squeue_codel:Fun(Time, Q, Codel),
    {Drops, NQ, State#state{codel=NCodel}};
handle(Time, Fun, Q, #state{timeout=Timeout, codel=Codel} = State) ->
    Result = queue:peek(Q),
    MinStart = Time - Timeout,
    {Drops, NQ, TimeoutNext} = timeout(Result, MinStart, Time, Q, Timeout, []),
    {Drops2, NQ2, NCodel} = squeue_codel:Fun(Time, NQ, Codel),
    {Drops2 ++ Drops, NQ2, State#state{timeout_next=TimeoutNext, codel=NCodel}}.

timeout(empty, _MinStart, Time, Q, _Timeout, Drops) ->
    %% The tail_time of the squeue is unknown (an item could be added in the
    %% past), so can not set a timeout_next.
    {Drops, Q, Time};
timeout({value, {Start, _}}, MinStart, _Time, Q, Timeout, Drops)
  when Start > MinStart ->
    %% Item is below sojourn timeout, it is the first item that can be
    %% dropped and it can't be dropped until it is above sojourn timeout.
    {Drops, Q, Start+Timeout};
timeout({value, Item}, MinStart, Time, Q, Timeout, Drops) ->
    %% Item is above sojourn timeout so drop it.
    NQ = queue:drop(Q),
    timeout(queue:peek(NQ), MinStart, Time, NQ, Timeout, [Item | Drops]).
