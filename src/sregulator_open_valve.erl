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
%% @doc Implements a simple valve with maximum capacity.
%%
%% `sregulator_open_value' can be used as the `sregulator_valve' in a
%% `sregulator'. It will provide a value that is always open up to a maximum
%% capacity. Its argument, `spec()' is of the form:
%% ```
%% #{max => Max :: non_neg_integer() | infinity} % default: infinity
%% '''
%% `Max' is the maximum number of concurrent tasks the valve will allow to run
%% (defaults to `infinity'). The valve remains open up to the maximum and then
%% closes. It ignores any updates.
-module(sregulator_open_valve).

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

-type spec() :: #{max => Max :: non_neg_integer() | infinity}.

-export_type([spec/0]).

-record(state, {max :: non_neg_integer() | infinity,
                small_time :: integer(),
                map :: sregulator_valve:internal_map()}).

%% sregulator_valve api

%% @private
-spec init(Map, Time, Spec) -> {open | closed, State, infinity} when
      Map :: sregulator_valve:internal_map(),
      Time :: integer(),
      Spec :: spec(),
      State :: #state{}.
init(Map, Time, Spec) ->
    Max = sbroker_util:max(Spec),
    handle(#state{max=Max, small_time=Time, map=Map}).

%% @private
-spec handle_ask(Pid, Ref, Time, State) ->
    {go, Open, open | closed, NState, infinity} when
      Pid :: pid(),
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      Open :: integer(),
      NState :: #state{}.
handle_ask(Pid, Ref, _, #state{max=Max, small_time=Small, map=Map} = State)
  when map_size(Map) < Max ->
    NMap = maps:put(Ref, Pid, Map),
    NState = State#state{map=NMap},
    case map_size(NMap) of
        Max ->
            {go, Small, closed, NState, infinity};
        _ ->
            {go, Small, open, NState, infinity}
    end.

%% @private
-spec handle_done(Ref, Time, State) ->
    {done | error, open | closed, NState, infinity} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_done(Ref, Time, #state{max=Max, map=Map} = State) ->
    Before = map_size(Map),
    NMap = maps:remove(Ref, Map),
    After = map_size(NMap),
    NState = State#state{map=NMap},
    if
        After < Before, Before =:= Max ->
            demonitor(Ref, [flush]),
            {done, open, NState#state{small_time=Time}, infinity};
        After < Before, After < Max ->
            demonitor(Ref, [flush]),
            {done, open, NState, infinity};
        After < Before ->
            demonitor(Ref, [flush]),
            {done, closed, NState, infinity};
        After < Max ->
            {error, open, NState, infinity};
        true ->
            {error, closed, NState, infinity}
    end.

%% @private
-spec handle_continue(Ref, Time, State) ->
    {go, Open, open | closed, NState, infinity} |
    {done | error, open | closed, NState, infinity} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      Open :: integer(),
      NState :: #state{}.
handle_continue(Ref, _, #state{max=Max, small_time=Small, map=Map} = State)
  when map_size(Map) < Max ->
    case maps:find(Ref, Map) of
        {ok, _} ->
            {go, Small, open, State, infinity};
        error ->
            {error, open, State, infinity}
    end;
handle_continue(Ref, Time, #state{max=Max, map=Map} = State)
  when map_size(Map) =:= Max ->
    case maps:find(Ref, Map) of
        {ok, _} ->
            {go, Time, closed, State, infinity};
        error ->
            {error, closed, State, infinity}
    end;
handle_continue(Ref, _, #state{map=Map} = State) ->
    Before = map_size(Map),
    NMap = maps:remove(Ref, Map),
    NState = State#state{map=NMap},
    case map_size(NMap) of
        Before ->
            {error, closed, NState, infinity};
        _ ->
            {done, closed, NState, infinity}
    end.

%% @private
-spec handle_update(Value, Time, State) ->
    {open | closed, NState, infinity} when
      Value :: integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_update(_, _, State) ->
    handle(State).

%% @private
-spec handle_info(Msg, Time, State) -> {open | closed, NState, infinity} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_info({'DOWN', Ref, _, _, _}, Time, #state{map=Map, max=Max} = State) ->
    Before = map_size(Map),
    NMap = maps:remove(Ref, Map),
    case map_size(NMap) of
        After when Before =:= Max, After < Max ->
            handle(State#state{map=NMap, small_time=Time});
        _ ->
            handle(State#state{map=NMap})
    end;
handle_info(_,  _, State) ->
    handle(State).

%% @private
-spec handle_timeout(Time, State) -> {open | closed, NState, infinity} when
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_timeout(_, State) ->
    handle(State).

%% @private
-spec code_change(OldVsn, Time, State, Extra) -> {Status, NState, infinity} when
      OldVsn :: any(),
      Time :: integer(),
      State :: #state{},
      Extra :: any(),
      Status :: open | closed,
      NState :: #state{}.
code_change(_, _, State, _) ->
    handle(State).

%% @private
-spec config_change(Spec, Time, State) -> {open | closed, NState, infinity} when
      Spec :: spec(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
config_change(Spec, _, State) ->
    Max = sbroker_util:max(Spec),
    handle(State#state{max=Max}).

%% @private
-spec size(State) -> Size when
      State :: #state{},
      Size :: non_neg_integer().
size(#state{map=Map}) ->
    map_size(Map).

%% @private
-spec open_time(State) -> Open | closed when
      State :: #state{},
      Open :: integer().
open_time(#state{map=Map, max=Max, small_time=Small})
  when map_size(Map) < Max ->
    Small;
open_time(#state{}) ->
    closed.

%% @private
-spec terminate(Reason, State) -> Map when
      Reason :: any(),
      State :: #state{},
      Map :: sregulator_valve:internal_map().
terminate(_, #state{map=Map}) ->
    Map.

%% Internal

handle(#state{max=Max, map=Map} = State) when map_size(Map) < Max ->
    {open, State, infinity};
handle(State) ->
    {closed, State, infinity}.
