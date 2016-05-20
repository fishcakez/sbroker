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
%% @doc Implements a valve which increases its size based on updates being below
%% a target between a minimum and maximum capacity.
%%
%% `sregulator_relative_value' can be used as the `sregulator_valve' in a
%% `sregulator'. Its argument is of the form:
%% ```
%% {Target :: integer(), Min :: non_neg_integer(),
%%  Max :: non_neg_integer() | infinity}
%% '''
%% `Target' is the target relative value in milliseconds. The valve will open
%% when an update below the target (in `native' time units) is received if
%% between the the minimum, `Min', and the maximum, `Max'. Once opened by an
%% update the valve remains open until the concurrency level increases by 1 or
%% an update greater than the target is received. The valve is always open when
%% below the minimum and always closed once it reaches the maximum.
%%
%% This valve tries to enforce a minimum level of concurrency and will grow
%% while a relevant `sbroker_queue' is moving quickly - up to a maximum.
%% Therefore this valves expects the updates to be samples from the
%% `RelativeTime' in `go' tuples. For example:
%% ```
%% {go, _Ref, _Value, RelativeTime, _SojournTime} = sbroker:ask(Broker),
%% sregulator:update(Regulator, RelativeTime).
%% '''
%% To grow when a queue is moving slowly use `-RelativeTime'.
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
-export([terminate/2]).

%% types

-record(state, {min :: non_neg_integer(),
                max :: non_neg_integer() | infinity,
                target :: integer(),
                last=init :: integer() | init | ask | continue,
                map :: sregulator_valve:internal_map()}).

%% sregulator_valve api

%% @private
-spec init(Map, Time, {RelativeTarget, Min, Max}) ->
    {open | closed, State, infinity} when
      Map :: sregulator_valve:internal_map(),
      Time :: integer(),
      RelativeTarget :: integer(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      State :: #state{}.
init(Map, _, {RelativeTarget, Min, Max}) ->
    {Min, Max} = sbroker_util:min_max(Min, Max),
    Target = sbroker_util:relative_target(RelativeTarget),
    handle(#state{min=Min, max=Max, target=Target, map=Map}).

%% @private
-spec handle_ask(Pid, Ref, Time, State) ->
    {open | closed, NState, infinity} when
      Pid :: pid(),
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_ask(Pid, Ref, _,
           #state{min=Min, max=Max, target=Target, last=Last,
                  map=Map} = State) ->
    NMap = maps:put(Ref, Pid, Map),
    case map_size(NMap) of
        Size when Size < Min ->
            {open, State#state{map=NMap}, infinity};
        %% Opened based on size
        Size when Size =:= Min, Min < Max, Last < Target ->
            {open, State#state{map=NMap}, infinity};
        Size when Size =:= Min ->
            {closed, State#state{map=NMap}, infinity};
        _ ->
            {closed, State#state{map=NMap, last=ask}, infinity}
    end.

%% @private
-spec handle_done(Ref, Time, State) ->
    {done | error, open | closed, NState, infinity} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_done(Ref, _, #state{map=Map} = State) ->
    Before = map_size(Map),
    done(Ref, Map, Before, State).

%% @private
-spec handle_continue(Ref, Time, State) ->
    {continue | done | error, open | closed, NState, infinity} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_continue(Ref, _,
                #state{min=Min, max=Max, target=Target, last=Last,
                       map=Map} = State) ->
    Size = map_size(Map),
    if
        Size =< Min ->
            continue(Ref, Map, Size, State, State);
        Size > Max ->
            done(Ref, Map, Size, State);
        Last < Target ->
            continue(Ref, Map, Size, State, State#state{last=continue});
        true ->
            done(Ref, Map, Size, State)
    end.

%% @private
-spec handle_update(Value, Time, State) ->
    {open | closed, NState, infinity} when
      Value :: integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_update(RelativeTime, _,
              #state{max=Max, target=Target, map=Map} = State)
  when RelativeTime < Target, map_size(Map) < Max ->
    {open, State#state{last=RelativeTime}, infinity};
handle_update(RelativeTime, _, #state{min=Min, map=Map} = State)
  when map_size(Map) < Min ->
    {open, State#state{last=RelativeTime}, infinity};
handle_update(RelativeTime, _, State) ->
    {closed, State#state{last=RelativeTime}, infinity}.

%% @private
-spec handle_info(Msg, Time, State) -> {open | closed, NState, infinity} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_info({'DOWN', Ref, process, _, _}, _, #state{map=Map} = State) ->
    NMap = maps:remove(Ref, Map),
    handle(map_size(NMap), State#state{map=NMap});
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
-spec config_change({RelativeTarget, Min, Max}, Time, State) ->
    {open | closed, NState, infinity} when
      RelativeTarget :: integer(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
config_change({RelativeTarget, Min, Max}, _, State) ->
    {Min, Max} = sbroker_util:min_max(Min, Max),
    Target = sbroker_util:relative_target(RelativeTarget),
    handle(State#state{min=Min, max=Max, target=Target}).

%% @private
-spec size(State) -> Size when
      State :: #state{},
      Size :: non_neg_integer().
size(#state{map=Map}) ->
    map_size(Map).

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

continue(Ref, Map, Size, ErrorState, OKState) ->
    case maps:find(Ref, Map) of
        {ok, _} ->
            {continue, status(Size, OKState), OKState, infinity};
        error ->
            {error, status(Size, ErrorState), ErrorState, infinity}
    end.

done(Ref, Map, Before, State) ->
    NMap = maps:remove(Ref, Map),
    NState = State#state{map=NMap},
    case map_size(NMap) of
        Before ->
            {error, status(Before, NState), NState, infinity};
        After ->
            demonitor(Ref, [flush]),
            {done, status(After, NState), NState, infinity}
    end.
