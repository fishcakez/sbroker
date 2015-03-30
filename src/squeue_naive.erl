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
-ifdef(LEGACY_TYPES).
-spec handle_timeout(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: integer(),
      Q :: queue().
-else.
-spec handle_timeout(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: integer(),
      Q :: queue:queue().
-endif.
handle_timeout(_Time, Q, undefined) ->
    {[], Q, undefined}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_out(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: integer(),
      Q :: queue().
-else.
-spec handle_out(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: integer(),
      Q :: queue:queue().
-endif.
handle_out(_Time, Q, undefined) ->
    {[], Q, undefined}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_out_r(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: integer(),
      Q :: queue().
-else.
-spec handle_out_r(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: integer(),
      Q :: queue:queue().
-endif.
handle_out_r(_Time, Q, undefined) ->
    {[], Q, undefined}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_join(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: integer(),
      Q :: queue().
-else.
-spec handle_join(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: integer(),
      Q :: queue:queue().
-endif.
handle_join(_Time, Q, undefined) ->
    {[], Q, undefined}.
