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
%% @doc Implements a naive queue management algorithm where no items are
%% dropped.
%%
%% `squeue_naive' can be used as the active queue management module in a
%% `squeue' queue. It ignores any argument given and does not queue management.
-module(squeue_naive).

-behaviour(squeue).

-export([init/2]).
-export([handle_timeout/3]).
-export([handle_out/3]).
-export([handle_out_r/3]).
-export([handle_join/3]).

%% @private
-spec init(Time, Args) -> undefined when
      Time :: integer(),
      Args :: any().
init(_Time, _Args) ->
    undefined.

%% @private
-spec handle_timeout(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: integer(),
      Q :: squeue:internal_queue(any()).
handle_timeout(_Time, Q, undefined) ->
    {[], Q, undefined}.

%% @private
-spec handle_out(Time, Q, undefined) ->
    {empty | {SojournTime, Item}, [], NQ, undefined} when
      Time :: integer(),
      Q :: squeue:internal_queue(Item),
      SojournTime :: non_neg_integer(),
      NQ :: squeue:internal_queue(Item).
handle_out(_Time, Q, undefined) ->
    case queue:out(Q) of
        {empty, NQ} ->
            {empty, [], NQ, undefined};
        {{value, Item}, NQ} ->
            {Item, [], NQ, undefined}
    end.

%% @private
-spec handle_out_r(Time, Q, undefined) ->
    {empty | {SojournTime, Item}, [], NQ, undefined} when
      Time :: integer(),
      Q :: squeue:internal_queue(Item),
      SojournTime :: non_neg_integer(),
      NQ :: squeue:internal_queue(Item).
handle_out_r(_Time, Q, undefined) ->
    case queue:out_r(Q) of
        {empty, NQ} ->
            {empty, [], NQ, undefined};
        {{value, Item}, NQ} ->
            {Item, [], NQ, undefined}
    end.

%% @private
-spec handle_join(Time, Q, undefined) -> undefined when
      Time :: integer(),
      Q :: squeue:internal_queue(any()).
handle_join(_Time, _Q, undefined) ->
    undefined.
