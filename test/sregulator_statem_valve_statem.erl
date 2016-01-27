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
-module(sregulator_statem_valve_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/1]).
-export([handle_update/3]).
-export([handle_ask/2]).
-export([handle/2]).
-export([config_change/3]).

module() ->
    sregulator_statem_valve.

args() ->
    ?SUCHTHAT(Opens, list(oneof([open, closed])), Opens =/= []).

init([Open | Rest] = Opens) ->
    {0, infinity, Open, {Rest, Opens}}.

handle_update(_, _, State) ->
    handle(State).

handle_ask(_, State) ->
    handle(State).

handle(_, State) ->
    handle(State).

config_change(Opens, _, {[Open | Rest], Opens}) ->
    {0, infinity, Open, {Rest, Opens}};
config_change(Opens, _, _) ->
    init(Opens).

%% Internal

handle({[], Opens}) ->
    handle({Opens, Opens});
handle({[Open | Rest], Opens}) ->
    {Open, {Rest, Opens}}.
