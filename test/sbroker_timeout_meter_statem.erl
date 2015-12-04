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
-module(sbroker_timeout_meter_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/2]).
-export([update_next/5]).
-export([update_post/5]).
-export([change/3]).
-export([timeout/2]).

module() ->
    sbroker_timeout_meter.

args() ->
    oneof([choose(0, 3), infinity]).

init(_, Timeout) ->
    Timeout.

update_next(Timeout, Time, _, _, _) ->
    handle(Timeout, Time).

update_post(Timeout, Time, _, _, _) ->
    {_, TimeoutTime} = handle(Timeout, Time),
    {true, TimeoutTime}.

change(_, Time, Timeout) ->
    handle(Timeout, Time).

timeout(Timeout, Time) ->
    {_, TimeoutTime} = handle(Timeout, Time),
    TimeoutTime.

%% Internal

handle(infinity, _) ->
    {infinity, infinity};
handle(Timeout, Time) ->
    {Timeout, Time + Timeout}.
