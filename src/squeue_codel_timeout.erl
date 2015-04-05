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
-spec handle_timeout(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: squeue:internal_queue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: squeue:internal_queue(Item),
      NState :: #state{}.
handle_timeout(Time, Q, #state{timeout_next=TimeoutNext, codel=Codel} = State)
  when TimeoutNext > Time ->
    {Drops, NQ, NCodel} = squeue_codel:handle_timeout(Time, Q, Codel),
    {Drops, NQ, State#state{codel=NCodel}};
handle_timeout(Time, Q, #state{codel=Codel, timeout=Timeout} = State) ->
    MinStart = Time - Timeout,
    case squeue_codel:handle_timeout(Time, Q, Codel) of
        {[{Start, _} | _] = Drops, NQ, NCodel} when Start > MinStart ->
            %% Min drop was fast and so no other items need to be dropped.
            NState = State#state{codel=NCodel, timeout_next=Start+Timeout},
            {Drops, NQ, NState};
        {Drops, NQ, NCodel} ->
            %% Min drop was slow or unknown and so may need to drop more items.
            timeout_loop(Time, NQ, Drops, MinStart, NCodel, State)
    end.

%% @private
-spec handle_out(Time, Q, State) ->
    {empty | {SojournTime, Item}, Drops, NQ, NState} when
      Time :: integer(),
      Q :: squeue:internal_queue(Item),
      State :: #state{},
      SojournTime :: non_neg_integer(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: squeue:internal_queue(Item),
      NState :: #state{}.
handle_out(Time, Q, #state{timeout_next=TimeoutNext, codel=Codel} = State)
  when TimeoutNext > Time ->
    {Result, Drops, NQ, NCodel} = squeue_codel:handle_out(Time, Q, Codel),
    {Result, Drops, NQ, State#state{codel=NCodel}};
handle_out(Time, Q, #state{timeout=Timeout, codel=Codel} = State) ->
    MinStart = Time - Timeout,
    case squeue_codel:handle_out(Time, Q, Codel) of
        {empty, Drops, NQ, NCodel} ->
            {empty, Drops, NQ, State#state{codel=NCodel}};
        {{Start, _} = Item, Drops, NQ, NCodel} when Start > MinStart ->
            %% Dequeued item was fast and so no other items need to be dropped.
            NState = State#state{codel=NCodel, timeout_next=Start+Timeout},
            {Item, Drops, NQ, NState};
        {Item, Drops, NQ, NCodel} ->
            %% Dequeued item was slow, drop it and may need to drop more items.
            out_loop(Time, NQ, [Item | Drops], MinStart, NCodel, State)
    end.

%% @private
-spec handle_out_r(Time, Q, State) ->
    {empty | {SojournTime, Item}, Drops, NQ, NState} when
      Time :: integer(),
      Q :: squeue:internal_queue(Item),
      State :: #state{},
      SojournTime :: non_neg_integer(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item :: any()}],
      NQ :: squeue:internal_queue(Item),
      NState :: #state{}.
handle_out_r(Time, Q, #state{timeout_next=TimeoutNext, codel=Codel} = State)
  when TimeoutNext > Time ->
    {Result, Drops, NQ, NCodel} = squeue_codel:handle_out_r(Time, Q, Codel),
    {Result, Drops, NQ, State#state{codel=NCodel}};
handle_out_r(Time, Q, #state{codel=Codel, timeout=Timeout} = State) ->
    MinStart = Time - Timeout,
    case squeue_codel:handle_out_r(Time, Q, Codel) of
        {empty, Drops, NQ, NCodel} ->
            {empty, Drops, NQ, State#state{codel=NCodel}};
        {{Start, _} = Item, [{Start2, _} | _] = Drops, NQ, NCodel}
          when Start > MinStart andalso Start2 > MinStart ->
            %% Min drop was fast and so no other items need to be dropped.
            NState = State#state{codel=NCodel, timeout_next=Start2+Timeout},
            {Item, Drops, NQ, NState};
        {{Start, _} = Item, Drops, NQ, NCodel} when Start > MinStart ->
            %% Min drop was slow or unknown and so may need to drop more items.
            out_r_loop(Time, NQ, Item, Drops, MinStart, NCodel, State);
        {Item, Drops, NQ, NCodel} ->
            %% Tail item is slow, so whole queue is slow! Drop everything!
            out_r_drop(Time, NQ, Item, Drops, NCodel, State)
    end.

%% @private
-spec handle_join(Time, Q, State) -> NState when
      Time :: integer(),
      Q :: squeue:internal_queue(any()),
      State :: #state{},
      NState :: #state{}.
handle_join(Time, Q, #state{codel=Codel} = State) ->
    NCodel = squeue_codel:handle_join(Time, Q, Codel),
    case queue:is_empty(Q) of
        true ->
            State#state{codel=NCodel, timeout_next=Time};
        false ->
            State#state{codel=NCodel}
    end.

%% Internal

timeout_loop(Time, Q, Drops, MinStart, Codel, State) ->
    {NDrops, NQ, TimeoutNext} = timeout_loop(Q, MinStart, Time, Drops),
    {NDrops, NQ, State#state{codel=Codel, timeout_next=TimeoutNext}}.

timeout_loop(Q, MinStart, Time, Drops) ->
    %% Codel won't drop or change state again unless time increases,
    %% an item is added, out is called on empty/an item below target or
    %% out_r is called on empty. None of these will occur in this loop as
    %% Timeout is greater than Target. Therefore do not need to call
    %% squeue_codel.
    timeout_loop(queue:peek(Q), Q, MinStart, Time, Drops).

timeout_loop(empty, Q, _, Time, Drops) ->
    {Drops, Q, Time};
timeout_loop({value, {Start, _}}, Q, MinStart, Time, Drops)
  when Start > MinStart ->
    Timeout = Time - MinStart,
    {Drops, Q, Start + Timeout};
timeout_loop({value, Item}, Q, MinStart, Time, Drops) ->
    NQ = queue:drop(Q),
    timeout_loop(queue:peek(NQ), NQ, MinStart, Time, [Item | Drops]).

out_loop(Time, Q, Drops, MinStart, Codel, State) ->
    {NDrops, NQ, TimeoutNext} = timeout_loop(Q, MinStart, Time, Drops),
    %% Need to call Codel to dequeue as this may update its state. No items are
    %% dropped as Codel only drops once per time (unless items are enqueued
    %% after dropping).
    {Result, [], NQ2, NCodel} = squeue_codel:handle_out(Time, NQ, Codel),
    {Result, NDrops, NQ2, State#state{codel=NCodel, timeout_next=TimeoutNext}}.

out_r_loop(Time, Q, Item, Drops, MinStart, Codel, State) ->
    {NDrops, NQ, TimeoutNext} = timeout_loop(Q, MinStart, Time, Drops),
    {Item, NDrops, NQ, State#state{codel=Codel, timeout_next=TimeoutNext}}.

out_r_drop(Time, Q, Item, Drops, Codel, State) ->
    Drops2 = queue:to_list(Q),
    NDrops = [Item | lists:reverse(Drops2, Drops)],
    NQ = queue:new(),
    %% Dequeue attempt on empty queue (after drops) stops Codel dropping.
    {empty, [], NQ2, NCodel} = squeue_codel:handle_out_r(Time, NQ, Codel),
    {empty, NDrops, NQ2, State#state{codel=NCodel}}.
