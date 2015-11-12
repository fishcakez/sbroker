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

-export([init/3]).
-export([handle_update/4]).
-export([handle_info/3]).
-export([config_change/4]).
-export([terminate/2]).

%% @private
-spec init(TimeUnit, Time, Timeout) -> Timeout when
      TimeUnit :: sbroker_time:unit(),
      Time :: integer(),
      Timeout :: timeout().
init(TimeUnit, _, Timeout) ->
    sbroker_util:timeout(Timeout, TimeUnit).

%% @private
-spec handle_update(ProcessTime, Count, Time, Timeout) ->
    {Timeout, Next} when
      ProcessTime :: non_neg_integer(),
      Count :: pos_integer(),
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
-spec config_change(TimeUnit, NTimeout, Time, Timeout) -> {NTimeout, Next} when
      TimeUnit :: sbroker_time:unit(),
      NTimeout :: timeout(),
      Time :: integer(),
      Timeout :: timeout(),
      Next :: integer() | infinity.
config_change(TimeUnit, NTimeout, Time, _) ->
    handle(Time, init(TimeUnit, Time, NTimeout)).

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
