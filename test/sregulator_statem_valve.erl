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
-module(sregulator_statem_valve).

-behaviour(sregulator_valve).

%% sregulator_valve_api

-export([init/3]).
-export([handle_ask/4]).
-export([handle_done/3]).
-export([handle_continue/3]).
-export([handle_update/3]).
-export([handle_info/3]).
-export([config_change/3]).
-export([size/1]).
-export([terminate/2]).

%% types

-record(state, {config :: [open | closed],
                opens :: [open | closed],
                map :: sregulator_valve:internal_map()}).

%% sregulator_valve api

init(Map, _, Opens) ->
    handle(#state{config=Opens, opens=Opens, map=Map}).

handle_ask(Pid, Ref, _, #state{map=Map} = State) ->
    NMap = maps:put(Ref, Pid, Map),
    handle(State#state{map=NMap}).

handle_done(Ref, _, #state{map=Map} = State) ->
    NMap = maps:remove(Ref, Map),
    {Open, NState} = handle(State#state{map=NMap}),
    case maps:is_key(Ref, Map) of
        true ->
            {done, Open, NState};
        false ->
            {error, Open, NState}
    end.

handle_continue(Ref, _, #state{map=Map} = State) ->
    {Open, NState} = handle(State),
    case maps:is_key(Ref, Map) of
        true when Open == open ->
            {Open2, NState2} = handle(NState),
            {continue, Open2, NState2};
        true when Open == closed ->
            NMap = maps:remove(Ref, Map),
            {Open2, NState2} = handle(NState#state{map=NMap}),
            {done, Open2, NState2};
        false ->
            {error, Open, NState}
    end.

handle_update(_, _, State) ->
    handle(State).

handle_info({'DOWN', Ref, process, _, _}, _, #state{map=Map} = State) ->
    NMap = maps:remove(Ref, Map),
    handle(State#state{map=NMap});
handle_info(_,  _, State) ->
    handle(State).

config_change(Opens, _, #state{config=Opens} = State) ->
    handle(State);
config_change(Opens, _, State) ->
    handle(State#state{config=Opens, opens=Opens}).

size(#state{map=Map}) ->
    map_size(Map).

terminate(_, #state{map=Map}) ->
    Map.

%% Internal

handle(#state{opens=[], config=[]} = State) ->
    {closed, State};
handle(#state{opens=[], config=Config} = State) ->
    handle(State#state{opens=Config});
handle(#state{opens=[Open|Opens]} = State) ->
    {Open, State#state{opens=Opens}}.
