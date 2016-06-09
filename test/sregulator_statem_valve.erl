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
-export([handle_timeout/2]).
-export([code_change/4]).
-export([config_change/3]).
-export([size/1]).
-export([open_time/1]).
-export([terminate/2]).

%% types

-record(state, {config :: [open | closed],
                opens :: [open | closed],
                open_time :: closed | integer(),
                map :: sregulator_valve:internal_map()}).

%% sregulator_valve api

init(Map, Time, Opens) ->
    handle(Time, #state{config=Opens, opens=Opens, map=Map, open_time=closed}).

handle_ask(Pid, Ref, Time, #state{map=Map, open_time=Open} = State) ->
    NMap = maps:put(Ref, Pid, Map),
    {Status, NState, Timeout} = handle(Time, State#state{map=NMap}),
    {go, Open, Status, NState, Timeout}.

handle_done(Ref, Time, #state{map=Map} = State) ->
    NMap = maps:remove(Ref, Map),
    {Open, NState, Timeout} = handle(Time, State#state{map=NMap}),
    case maps:is_key(Ref, Map) of
        true ->
            {done, Open, NState, Timeout};
        false ->
            {error, Open, NState, Timeout}
    end.

handle_continue(Ref, Time, #state{map=Map} = State) ->
    {Open, NState, Timeout} = handle(Time, State),
    case maps:is_key(Ref, Map) of
        true when Open == open ->
            {Open2, NState2, Timeout2} = handle(Time, NState),
            {go, Time, Open2, NState2, Timeout2};
        true when Open == closed ->
            NMap = maps:remove(Ref, Map),
            {Open2, NState2, Timeout2} = handle(Time, NState#state{map=NMap}),
            {done, Open2, NState2, Timeout2};
        false ->
            {error, Open, NState, Timeout}
    end.

handle_update(_, Time, State) ->
    handle(Time, State).

handle_info({'DOWN', Ref, process, _, _}, Time, #state{map=Map} = State) ->
    NMap = maps:remove(Ref, Map),
    handle(Time, State#state{map=NMap});
handle_info(_,  Time, State) ->
    handle(Time, State).

handle_timeout(Time, State) ->
    handle(Time, State).

code_change(_, Time, #state{config=Opens} = State, _) ->
    handle(Time, State#state{config=Opens, opens=Opens}).

config_change(Opens, Time, #state{config=Opens} = State) ->
    handle(Time, State);
config_change(Opens, Time, State) ->
    handle(Time, State#state{config=Opens, opens=Opens}).

size(#state{map=Map}) ->
    map_size(Map).

open_time(#state{open_time=Open}) ->
    Open.

terminate(_, #state{map=Map}) ->
    Map.

%% Internal

handle(_, #state{opens=[], config=[]} = State) ->
    {closed, State#state{open_time=closed}, infinity};
handle(Time, #state{opens=[], config=Config} = State) ->
    handle(Time, State#state{opens=Config});
handle(Time, #state{opens=[open|Opens]} = State) ->
    {open, State#state{opens=Opens, open_time=Time}, infinity};
handle(_, #state{opens=[closed|Opens]} = State) ->
    {closed, State#state{opens=Opens, open_time=closed}, infinity}.
