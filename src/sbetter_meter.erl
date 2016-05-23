%%-------------------------------------------------------------------
%%
%% Copyright (c) 2016, James Fish <james@fishcakez.com>
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
%% @doc Registers the process with and updates the `sbetter_server' with
%% approximate queue sojourn times for use with the `sbetter' load balancer.
%%
%% `sbetter_meter' can be used as the `sbroker_meter' in a `sbroker' or
%% a `sregulator'. Its argument can take any form and is ignored.
%% @see sbetter
%% @see sbetter_server
-module(sbetter_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/5]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([terminate/2]).

%% @private
-spec init(Time, Arg) -> {Pid, Time} when
      Time :: integer(),
      Arg :: any(),
      Pid :: pid().
init(Time, _) ->
    Pid = self(),
    true = sbetter_server:register(Pid),
    % Want to update immediately as default value is max small int on 64bit VM.
    {Pid, Time}.

%% @private
-spec handle_update(QueueDelay, ProcessDelay, RelativeTime, Time, Pid) ->
    {Pid, infinity} when
      QueueDelay :: non_neg_integer(),
      ProcessDelay :: non_neg_integer(),
      RelativeTime :: integer(),
      Time :: integer(),
      Pid :: pid().
handle_update(QueueDelay, ProcessDelay, RelativeTime, _, Pid) ->
    AskSojourn = sojourn(QueueDelay + ProcessDelay, RelativeTime),
    AskRSojourn = sojourn(QueueDelay + ProcessDelay, -RelativeTime),
    true = sbetter_server:update(Pid, AskSojourn, AskRSojourn),
    {Pid, infinity}.

%% @private
-spec handle_info(Msg, Time, Pid) -> {Pid, infinity} when
      Msg :: any(),
      Time :: integer(),
      Pid :: pid().
handle_info(_, _, Pid) ->
    {Pid, infinity}.

%% @private
-spec code_change(OldVsn, Time, Pid, Extra) -> {Pid, infinity} when
      OldVsn :: any(),
      Time :: integer(),
      Pid :: pid(),
      Extra :: any().
code_change(_, _, Pid, _) ->
    {Pid, infinity}.

%% @private
-spec config_change(Arg, Time, Pid) -> {Pid, infinity} when
      Arg :: term(),
      Time :: integer(),
      Pid :: pid().
config_change(_, _, Pid) ->
    {Pid, infinity}.

%% @private
-spec terminate(Reason, Pid) -> true when
      Reason :: any(),
      Pid :: pid().
terminate(_, Pid) ->
    sbetter_server:unregister(Pid).

%% Internal

sojourn(QueueDelay, RelativeTime) ->
    QueueDelay + max(RelativeTime, 0).
