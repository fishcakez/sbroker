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
%% @doc Implements a basic feedback loop where items are dequeued when the
%% sojourn time of another queue is less than a timeout valve.
%%
%% `svalve_timeout' can be used as the feedback loop module in a `svalve' queue.
%% It's argument is a `timeout()', which is the timeout valve, i.e. the maximum
%% sojourn time of the the other at which items are dequeued from the queue.
-module(svalve_timeout).

-behaviour(svalve).

-export([init/1]).
-export([handle_sojourn/4]).
-export([handle_sojourn_r/4]).
-export([handle_sojourn_closed/4]).
-export([handle_dropped/3]).
-export([handle_dropped_r/3]).
-export([handle_dropped_closed/3]).

%% @private
-spec init(Timeout) -> Timeout when
      Timeout :: timeout().
init(Timeout) ->
    Timeout.

%% @private
-spec handle_sojourn(Time, SojournTime, S, Timeout) ->
    {Result, Drops, NS, Timeout} when
      Time :: non_neg_integer(),
      SojournTime :: non_neg_integer(),
      S :: squeue:squeue(Item),
      Timeout :: timeout(),
      Result :: closed | empty | {ItemSojournTime, Item},
      ItemSojournTime :: non_neg_integer(),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_sojourn(Time, SojournTime, S, Timeout) when SojournTime > Timeout ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, Timeout};
handle_sojourn(Time, _, S, Timeout) ->
    {Result, Drops, NS} = squeue:out(Time, S),
    {Result, Drops, NS, Timeout}.

%% @private
-spec handle_sojourn_r(Time, SojournTime, S, Timeout) ->
    {Result, Drops, NS, Timeout} when
      Time :: non_neg_integer(),
      SojournTime :: non_neg_integer(),
      S :: squeue:squeue(Item),
      Timeout :: timeout(),
      Result :: closed | empty | {ItemSojournTime, Item},
      ItemSojournTime :: non_neg_integer(),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_sojourn_r(Time, SojournTime, S, Timeout) when SojournTime > Timeout ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, Timeout};
handle_sojourn_r(Time, _, S, Timeout) ->
    {Result, Drops, NS} = squeue:out_r(Time, S),
    {Result, Drops, NS, Timeout}.

%% @private
-spec handle_sojourn_closed(Time, SojournTime, S, Timeout) ->
    {closed, Drops, NS, Timeout} when
      Time :: non_neg_integer(),
      SojournTime :: non_neg_integer(),
      S :: squeue:squeue(Item),
      Timeout :: timeout(),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_sojourn_closed(Time, _, S, Timeout) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, Timeout}.

%% @private
-spec handle_dropped(Time, S, Timeout) ->
    {closed, Drops, NS, Timeout} when
      Time :: non_neg_integer(),
      S :: squeue:squeue(Item),
      Timeout :: timeout(),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_dropped(Time, S, Timeout) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, Timeout}.

%% @private
-spec handle_dropped_r(Time, S, Timeout) ->
    {closed, Drops, NS, Timeout} when
      Time :: non_neg_integer(),
      S :: squeue:squeue(Item),
      Timeout :: timeout(),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_dropped_r(Time, S, Timeout) ->
    handle_dropped(Time, S, Timeout).

%% @private
-spec handle_dropped_closed(Time, S, Timeout) ->
    {closed, Drops, NS, Timeout} when
      Time :: non_neg_integer(),
      S :: squeue:squeue(Item),
      Timeout :: timeout(),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_dropped_closed(Time, S, Timeout) ->
    handle_dropped(Time, S, Timeout).
