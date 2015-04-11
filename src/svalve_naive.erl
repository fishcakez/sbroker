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
%% @doc Implements a naive feedback loop that attempts to dequeue an item every
%% time another queue dequeues an item.
%%
%% `svalve_naive' can be used as the feedback loop module in a `svalve' queue.
%% It ignores any argument given.
-module(svalve_naive).

-behaviour(svalve).

-export([init/2]).
-export([handle_sojourn/4]).
-export([handle_sojourn_r/4]).
-export([handle_sojourn_closed/4]).
-export([handle_dropped/3]).
-export([handle_dropped_r/3]).
-export([handle_dropped_closed/3]).

%% @private
-spec init(Time, Args) -> undefined when
      Time :: integer(),
      Args :: any().
init(_Time, _Args) ->
    undefined.

%% @private
-spec handle_sojourn(Time, SojournTime, S, undefined) ->
    {Result, Drops, NS, undefined} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      S :: squeue:squeue(Item),
      Result :: empty | {ItemSojournTime, Item},
      ItemSojournTime :: non_neg_integer(),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_sojourn(_, _, S, undefined) ->
    {Result, Drops, NS} = squeue:out(S),
    {Result, Drops, NS, undefined}.

%% @private
-spec handle_sojourn_r(Time, SojournTime, S, undefined) ->
    {Result, Drops, NS, undefined} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      S :: squeue:squeue(Item),
      Result :: empty | {ItemSojournTime, Item},
      ItemSojournTime :: non_neg_integer(),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_sojourn_r(_, _, S, undefined) ->
    {Result, Drops, NS} = squeue:out_r(S),
    {Result, Drops, NS, undefined}.

%% @private
-spec handle_sojourn_closed(Time, SojournTime, S, undefined) ->
    {closed, Drops, NS, undefined} when
      Time :: integer(),
      SojournTime :: non_neg_integer(),
      S :: squeue:squeue(Item),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_sojourn_closed(_, _, S, undefined) ->
    {Drops, NS} = squeue:timeout(S),
    {closed, Drops, NS, undefined}.

%% @private
-spec handle_dropped(Time, S, undefined) ->
    {closed, Drops, NS, undefined} when
      Time :: integer(),
      S :: squeue:squeue(Item),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_dropped(_, S, undefined) ->
    {Drops, NS} = squeue:timeout(S),
    {closed, Drops, NS, undefined}.

%% @private
-spec handle_dropped_r(Time, S, undefined) ->
    {closed, Drops, NS, undefined} when
      Time :: integer(),
      S :: squeue:squeue(Item),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_dropped_r(Time, S, undefined) ->
    handle_dropped(Time, S, undefined).

%% @private
-spec handle_dropped_closed(Time, S, undefined) ->
    {closed, Drops, NS, undefined} when
      Time :: integer(),
      S :: squeue:squeue(Item),
      Drops :: [{DropSojournTime, Item}],
      DropSojournTime :: non_neg_integer(),
      NS :: squeue:squeue(Item).
handle_dropped_closed(Time, S, undefined) ->
    handle_dropped(Time, S, undefined).
