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
%% @doc Implements a basic queue management algorithm where items are dropped
%% once their sojourn time is greater than a timeout value.
%%
%% `squeue_timeout' can be used as the active queue management module in a
%% `squeue' queue. It's argument is a `non_neg_integer()', which is the timeout
%% value, i.e. the minimum sojourn time at which items are dropped from the
%% queue.
-module(squeue_timeout).

-behaviour(squeue).

-export([init/2]).
-export([handle_timeout/3]).
-export([handle_out/3]).
-export([handle_out_r/3]).
-export([handle_join/3]).

-record(state, {timeout :: non_neg_integer(),
                timeout_next :: integer()}).

%% @private
-spec init(Time, Timeout) -> State when
      Time :: integer(),
      Timeout ::non_neg_integer(),
      State :: #state{}.
init(Time, Timeout) when is_integer(Timeout) andalso Timeout >= 0 ->
    #state{timeout=Timeout, timeout_next=Time+Timeout}.

%% @private
-spec handle_timeout(Time, Q, State) -> {Drops, NQ, NState} when
      Time :: integer(),
      Q :: squeue:internal_queue(Item),
      State :: #state{},
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NQ :: squeue:internal_queue(Item),
      NState :: #state{}.
handle_timeout(Time, Q, #state{timeout_next=TimeoutNext} = State)
  when Time < TimeoutNext ->
    {[], Q, State};
handle_timeout(Time, Q, #state{timeout=Timeout} = State) ->
    timeout(queue:peek(Q), Time - Timeout, Time, Q, State, []).

%% @private
-spec handle_out(Time, Q, State) ->
    {empty | {SojournTime, Item}, Drops, NQ, NState} when
      Time :: integer(),
      Q :: squeue:internal_queue(Item),
      State :: #state{},
      SojournTime :: non_neg_integer(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NQ :: squeue:internal_queue(Item),
      NState :: #state{}.
handle_out(Time, Q, State) ->
    {Drops, NQ, NState} = handle_timeout(Time, Q, State),
    case queue:out(NQ) of
        {empty, NQ2} ->
            {empty, Drops, NQ2, NState};
        {{value, Item}, NQ2} ->
            {Item, Drops, NQ2, NState}
    end.

%% @private
-spec handle_out_r(Time, Q, State) ->
    {empty | {SojournTime, Item}, Drops, NQ, NState} when
      Time :: integer(),
      Q :: squeue:internal_queue(Item),
      State :: #state{},
      SojournTime :: non_neg_integer(),
      Drops :: [{DropSojournTime :: non_neg_integer(), Item}],
      NQ :: squeue:internal_queue(Item),
      NState :: #state{}.
handle_out_r(Time, Q, State) ->
    {Drops, NQ, NState} = handle_timeout(Time, Q, State),
    case queue:out_r(NQ) of
        {empty, NQ2} ->
            {empty, Drops, NQ2, NState};
        {{value, Item}, NQ2} ->
            {Item, Drops, NQ2, NState}
    end.

%% @private
-spec handle_join(Time, Q, State) -> NState when
      Time :: integer(),
      Q :: squeue:internal_queue(any()),
      State :: #state{},
      NState :: #state{}.
handle_join(Time, Q, State) ->
    case queue:is_empty(Q) of
        true ->
            State#state{timeout_next=Time};
        false ->
            State
    end.

%% Internal

timeout(empty, _MinStart, _Time, Q, State, Drops) ->
    %% The tail_time of the squeue is unknown (an item could be added in the
    %% past), so can not set a timeout_next.
    {Drops, Q, State};
timeout({value, {Start, _}}, MinStart, _Time, Q,
        #state{timeout=Timeout} = State, Drops) when Start > MinStart ->
    %% Item is below sojourn timeout, it is the first item that can be
    %% dropped and it can't be dropped until it is above sojourn timeout.
    {Drops, Q, State#state{timeout_next=Start+Timeout}};
timeout({value, Item}, MinStart, Time, Q, State, Drops) ->
    %% Item is above sojourn timeout so drop it.
    NQ = queue:drop(Q),
    timeout(queue:peek(NQ), MinStart, Time, NQ, State, [Item | Drops]).
