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
-module(sregulator_open_valve_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/3]).
-export([handle_update/3]).
-export([handle_ask/2]).
-export([handle_done/2]).
-export([handle/2]).
-export([config_change/4]).

module() ->
    sregulator_open_valve.

args() ->
    ?LET(Max, oneof([choose(0, 5), infinity]), #{max => Max}).

init(#{max := Max}, _, _) ->
    {0, Max, open, undefined}.

handle_update(_, _, State) ->
    {open, State}.

handle_ask(_, State) ->
    {open, State}.

handle_done(_, State) ->
    {open, State}.

handle(_, State) ->
    {open, State}.

config_change(#{max := Max}, _, _, undefined) ->
    {0, Max, open, undefined}.
