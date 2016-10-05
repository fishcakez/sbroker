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
%% @doc Implements a valve which increases its size when an update is below
%% a target.
%%
%% `sregulator_relative_value' can be used as the `sregulator_valve' in a
%% `sregulator'. It will provide a valve that increases in size when an update
%% is below a target valve between the minimum and maximum capacity. Its
%% argument, `spec()', is of the form:
%% ```
%% #{target   => Target :: integer(), % default: 100
%%   min      => Min :: non_neg_integer(), % default: 0
%%   max      => Max :: non_neg_integer() | infinity} % default: infinity
%% '''
%% `Target' is the target relative value in milliseconds (defaults to `100').
%% The valve will open when an update below the target (in `native' time units)
%% is received if between the the minimum, `Min' (defaults to `0'), and the
%% maximum, `Max' (defaults to `infinity'). Once opened by an update the valve
%% remains open until the concurrency level increases by 1 or an update greater
%% than the target is received. The valve is always open when below the minimum
%% and always closed once it reaches the maximum.
%%
%% This valve tries to enforce a minimum level of concurrency and will grow
%% while a relevant `sbroker_queue' is moving quickly - up to a maximum.
%% Therefore this valve expects the updates to be from a
%% `sregulator_update_meter'.
%%
%% By using a positive target `RelativeTime' of an `sbroker' queue, a pool of
%% workers can grow before the queue is empty. Therefore preventing or delaying
%% the situation where the counter party client processes have to wait for a
%% worker.
%%
%% By using a negative target `RelativeTime' of an `sbroker' queue, a pool of
%% expensive resources can delay growth until required.
-module(sregulator_relative_valve).

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

-type spec() ::
    #{target   => Target :: integer(),
      min      => Min :: non_neg_integer(),
      max      => Max :: non_neg_integer() | infinity}.

-export_type([spec/0]).

-record(state, {min :: non_neg_integer(),
                max :: non_neg_integer() | infinity,
                target :: integer(),
                last=init :: integer() | init | ask | continue,
                updated :: integer(),
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
    {Min, Max} = sbroker_util:min_max(Spec),
    Target = sbroker_util:relative_target(Spec),
    handle(#state{min=Min, max=Max, target=Target, updated=Time,
                  small_time=Time, map=Map}).

%% @private
-spec handle_ask(Pid, Ref, Time, State) ->
    {go, OpenTime, open | closed, NState, infinity} when
      Pid :: pid(),
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      OpenTime :: integer(),
      NState :: #state{}.
handle_ask(Pid, Ref, _,
           #state{min=Min, max=Max, target=Target, last=Last, updated=Updated,
                  small_time=Small, map=Map} = State) ->
    NMap = maps:put(Ref, Pid, Map),
    case map_size(NMap) of
        Size when Size < Min ->
            {go, Small, open, State#state{map=NMap}, infinity};
        %% Opened based on size
        Size when Size =:= Min, Min < Max, Last < Target ->
            {go, Small, open, State#state{map=NMap}, infinity};
        Size when Size =:= Min ->
            {go, Small, closed, State#state{map=NMap}, infinity};
        _ when Last < Target ->
            {go, Updated, closed, State#state{map=NMap, last=ask}, infinity}
    end.

%% @private
-spec handle_done(Ref, Time, State) ->
    {done | error, open | closed, NState, infinity} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_done(Ref, Time, #state{map=Map} = State) ->
    Before = map_size(Map),
    done(Ref, Map, Before, Time, State).

%% @private
-spec handle_continue(Ref, Time, State) ->
    {go, Open, open | closed, NState, infinity} |
    {done | error, open | closed, NState, infinity} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      Open :: integer(),
      NState :: #state{}.
handle_continue(Ref, Time,
                #state{min=Min, max=Max, target=Target, last=Last,
                       updated=Updated, small_time=Small, map=Map} = State) ->
    Size = map_size(Map),
    if
        Size < Min ->
            continue(Ref, Map, Size, Small, State, State);
        Size =:= Min ->
            continue(Ref, Map, Size, Time, State, State);
        Size > Max ->
            done(Ref, Map, Size, Time, State);
        Last < Target ->
            NState = State#state{last=continue},
            continue(Ref, Map, Size, Updated, State, NState);
        true ->
            done(Ref, Map, Size, Time, State)
    end.

%% @private
-spec handle_update(Value, Time, State) ->
    {open | closed, NState, infinity} when
      Value :: integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_update(RelativeTime, Time,
              #state{max=Max, target=Target, map=Map} = State)
  when RelativeTime < Target, map_size(Map) < Max ->
    {open, State#state{last=RelativeTime, updated=Time}, infinity};
handle_update(RelativeTime, Time, #state{min=Min, map=Map} = State)
  when map_size(Map) < Min ->
    {open, State#state{last=RelativeTime, updated=Time}, infinity};
handle_update(RelativeTime, Time, State) ->
    {closed, State#state{last=RelativeTime, updated=Time}, infinity}.

%% @private
-spec handle_info(Msg, Time, State) -> {open | closed, NState, infinity} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_info({'DOWN', Ref, _, _, _}, Time, #state{map=Map, min=Min} = State) ->
    Before = map_size(Map),
    NMap = maps:remove(Ref, Map),
    case map_size(NMap) of
        After when Before =:= Min, After < Min ->
            handle(State#state{map=NMap, small_time=Time});
        _ ->
            handle(State#state{map=NMap})
    end;
handle_info(_, _, State) ->
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
    {Min, Max} = sbroker_util:min_max(Spec),
    Target = sbroker_util:relative_target(Spec),
    handle(State#state{min=Min, max=Max, target=Target}).

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
open_time(#state{map=Map, min=Min, small_time=Small})
  when map_size(Map) < Min ->
    Small;
open_time(#state{map=Map, max=Max, target=Target, last=Last, updated=Updated})
  when map_size(Map) < Max, Last < Target ->
    Updated;
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

handle(#state{map=Map} = State) ->
    handle(map_size(Map), State).

handle(Size, State) ->
    {status(Size, State), State, infinity}.

status(Size, #state{min=Min}) when Size < Min ->
    open;
status(Size, #state{target=Target, last=Last, max=Max})
  when Last < Target, Size < Max ->
    open;
status(_, _) ->
    closed.

continue(Ref, Map, Size, Open, ErrorState, OKState) ->
    case maps:find(Ref, Map) of
        {ok, _} ->
            {go, Open, status(Size, OKState), OKState, infinity};
        error ->
            {error, status(Size, ErrorState), ErrorState, infinity}
    end.

done(Ref, Map, Before, Time, #state{min=Min} = State) ->
    NMap = maps:remove(Ref, Map),
    NState = State#state{map=NMap},
    case map_size(NMap) of
        Before ->
            {error, status(Before, NState), NState, infinity};
        _ when Before =:= Min ->
            {done, open, NState#state{small_time=Time}, infinity};
        After ->
            demonitor(Ref, [flush]),
            {done, status(After, NState), NState, infinity}
    end.
