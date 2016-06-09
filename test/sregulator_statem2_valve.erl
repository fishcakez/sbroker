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
-module(sregulator_statem2_valve).

-behaviour(sregulator_valve).

%% sregulator_valve_api

-export([init/3]).
-export([handle_ask/4]).
-export([handle_done/3]).
-export([handle_continue/3]).
-export([handle_update/3]).
-export([handle_info/3]).
-export([handle_timeout/2]).
-export([code_change/4]).
-export([config_change/3]).
-export([size/1]).
-export([open_time/1]).
-export([terminate/2]).

%% sregulator_valve api

init(Map, Time, Opens) ->
    sregulator_statem_valve:init(Map, Time, Opens).

handle_ask(Pid, Ref, Time, State) ->
    sregulator_statem_valve:handle_ask(Pid, Ref, Time, State).

handle_done(Ref, Time, State) ->
    sregulator_statem_valve:handle_done(Ref, Time, State).

handle_continue(Ref, Time, State) ->
    sregulator_statem_valve:handle_continue(Ref, Time, State).

handle_update(Value, Time, State) ->
    sregulator_statem_valve:handle_update(Value, Time, State).

handle_info(Msg, Time, State) ->
    sregulator_statem_valve:handle_info(Msg, Time, State).

handle_timeout(Time, State) ->
     sregulator_statem_valve:handle_timeout(Time, State).

code_change(OldVsn, Time, State, Extra) ->
    sregulator_statem_valve:code_change(OldVsn, Time, State, Extra).

config_change(Opens, Time, State) ->
    sregulator_statem_valve:config_change(Opens, Time, State).

size(State) ->
    sregulator_statem_valve:size(State).

open_time(State) ->
    sregulator_statem_valve:open_time(State).

terminate(Reason, State) ->
    sregulator_statem_valve:terminate(Reason, State).
