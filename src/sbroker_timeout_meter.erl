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
-module(sbroker_timeout_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/4]).
-export([handle_info/3]).
-export([config_change/3]).
-export([terminate/2]).

%% @private
-spec init(Time, Timeout) -> Timeout when
      Time :: integer(),
      Timeout :: timeout().
init(_, Timeout) ->
    sbroker_util:timeout(Timeout).

%% @private
-spec handle_update(QueueDelay, ProcessDelay, Time, Timeout) ->
    {Timeout, Next} when
      QueueDelay :: non_neg_integer(),
      ProcessDelay :: non_neg_integer(),
      Time :: integer(),
      Timeout :: timeout(),
      Next :: integer() | infinity.
handle_update(_, _, Time, Timeout) ->
    handle(Time, Timeout).

%% @private
-spec handle_info(Msg, Time, Timeout) -> {Timeout, Next} when
      Msg :: any(),
      Time :: integer(),
      Timeout :: timeout(),
      Next :: integer() | infinity.
handle_info(_, Time, Timeout) ->
    handle(Time, Timeout).

%% @private
-spec config_change(NTimeout, Time, Timeout) -> {NTimeout, Next} when
      NTimeout :: timeout(),
      Time :: integer(),
      Timeout :: timeout(),
      Next :: integer() | infinity.
config_change(NTimeout, Time, _) ->
    handle(Time, init(Time, NTimeout)).

%% @private
-spec terminate(Reason, Timeout) -> ok when
      Reason :: any(),
      Timeout :: timeout().
terminate(_, _) ->
    ok.

%% Internal

handle(_, infinity) ->
    {infinity, infinity};
handle(Time, Timeout) ->
    {Timeout, Time + Timeout}.
