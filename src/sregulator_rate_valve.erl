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
%% @doc Implements a valve with a rate limit between a minimum and maximum
%% capacity.
%%
%% `sregulator_rate_value' can be used as the `sregulator_valve' in a
%% `sregulator'. It will provide a valve that limits the rate of tasks when
%% between a minimum and maximum capacity. Its argument, `spec()', is of the
%% form:
%% ```
%% #{limit    => non_neg_integer(), % default: 100
%%   interval => pos_integer(), % default: 1000
%%   min      => non_neg_integer(), % default: 0
%%   max      => non_neg_integer() | infinity}. % default: infinity
%% '''
%% `Limit' is the number of new tasks (defaults to `100') that can run when the
%% number of concurrent tasks is at or above the minimum capacity, `Min'
%% (defaults to `0'), and below the maximum capacity, `Max' (defaults to
%% `infinity'). The limit is temporarily reduced by `1' for `Interval'
%% milliseconds (defaults to `1000') after a task is done and the capacity is
%% above `Min'. Also the limit is restricted to `0' for `Interval' milliseconds
%% after the valve is started. This ensures that no more than `Limit' tasks can
%% run above the minimum capacity for any time interval of `Interval'
%% milliseconds, even if the `sregulator' is restarted.
%%
%% This valve ignores any updates.
-module(sregulator_rate_valve).

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
    #{limit    => non_neg_integer(),
      interval => pos_integer(),
      min      => non_neg_integer(),
      max      => non_neg_integer() | infinity}.

-export_type([spec/0]).

-record(state, {min :: non_neg_integer(),
                max :: non_neg_integer() | infinity,
                interval :: pos_integer(),
                overflow = 0 :: non_neg_integer(),
                opens :: queue:queue(integer()),
                open_next :: integer() | infinity,
                small_time :: integer(),
                map :: sregulator_valve:internal_map()}).

%% sregulator_valve api

%% @private
-spec init(Map, Time, Spec) ->
    {open, State, infinity} |
    {closed, State, NextTimeout} when
      Map :: sregulator_valve:internal_map(),
      Time :: integer(),
      Spec :: spec(),
      State :: #state{},
      NextTimeout :: integer() | infinity.
init(Map, Time, Spec) ->
    Limit = sbroker_util:limit(Spec),
    Interval = sbroker_util:interval(Spec),
    {Min, Max} = sbroker_util:min_max(Spec),
    Open = Time + Interval,
    {Overflow, Opens, Next} = change(Map, Limit, 0, Open, Min, queue:new()),
    State = #state{min=Min, max=Max, interval=Interval, overflow=Overflow,
                   opens=Opens, open_next=Next, small_time=Time, map=Map},
    handle(Time, State).

%% @private
-spec handle_ask(Pid, Ref, Time, State) ->
    {go, Open, open, NState, infinity} |
    {go, Open, closed, NState, NextTimeout} when
      Pid :: pid(),
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      Open :: integer(),
      NState :: #state{},
      NextTimeout :: integer() | infinity.
handle_ask(Pid, Ref, Time,
           #state{min=Min, open_next=Next, small_time=Small,
                  map=Map} = State) ->
    NMap = maps:put(Ref, Pid, Map),
    case map_size(NMap) of
        Size when Size < Min ->
            {go, min(Small, Next), open, State#state{map=NMap}, infinity};
        Min ->
            NState = State#state{map=NMap},
            {Status, NextTimeout} = status(Min, Time, NState),
            {go, min(Small, Next), Status, NState, NextTimeout};
        Size when Time >= Next ->
            NState = limit_go(NMap, State),
            {Status, NextTimeout} = status(Size, Time, NState),
            {go, Next, Status, NState, NextTimeout}
    end.

%% @private
-spec handle_done(Ref, Time, State) ->
    {done | error, open, NState, infinity} |
    {done | error, closed, NState, NextTimeout} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      NextTimeout :: integer() | infinity.
handle_done(Ref, Time, #state{map=Map} = State) ->
    done(Ref, Map, map_size(Map), Time, State).

%% @private
-spec handle_continue(Ref, Time, State) ->
    {go, Open, open, NState, infinity} |
    {go, Open, closed, NState, NextTimeout} |
    {done | error, open, NState, infinity} |
    {done | error, closed, NState, NextTimeout} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      Open :: integer(),
      NState :: #state{},
      NextTimeout :: integer().
handle_continue(Ref, Time,
                #state{min=Min, max=Max, small_time=Small, open_next=Next,
                       map=Map} = State) ->
    Size = map_size(Map),
    if
        Size < Min ->
            continue(Ref, Map, Size, Small, Time, State, State);
        Size =:= Min ->
            continue(Ref, Map, Size, Time, Time, State, State);
        Size > Max; Time < Next ->
            done(Ref, Map, Size, Time, State);
        true ->
            NState = limit_continue(Time, State),
            continue(Ref, Map, Size, Small, Time, State, NState)
    end.

%% @private
-spec handle_update(Value, Time, State) ->
    {open, NState, infinity} |
    {closed, NState, NextTimeout} when
      Value :: integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      NextTimeout :: integer() | infinity.
handle_update(_, Time, State) ->
    handle(Time, State).

%% @private
-spec handle_info(Msg, Time, State) ->
    {open, NState, infinity} |
    {closed, NState, NextTimeout} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      NextTimeout :: integer() | infinity.
handle_info({'DOWN', Ref, _, _, _}, Time, #state{map=Map, min=Min} = State) ->
    Before = map_size(Map),
    NMap = maps:remove(Ref, Map),
    case map_size(NMap) of
        Before ->
            handle(Time, State#state{map=NMap});
        _ when Before =:= Min ->
            {open, State#state{map=NMap, small_time=Time}, infinity};
        _ when Before < Min ->
            {open, State#state{map=NMap}, infinity};
        _ ->
            handle(Time, limit_done(NMap, Time, State))
    end;
handle_info(_,  Time, State) ->
    handle(Time, State).

%% @private
-spec handle_timeout(Time, State) ->
    {open, NState, infinity} |
    {closed, NState, NextTimeout} when
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      NextTimeout :: integer() | infinity.
handle_timeout(Time, State) ->
    handle(Time, State).

%% @private
-spec code_change(OldVsn, Time, State, Extra) ->
    {open, NState, infinity} |
    {closed, NState, NextTimeout} when
      OldVsn :: any(),
      Time :: integer(),
      State :: #state{},
      Extra :: any(),
      NState :: #state{},
      NextTimeout :: integer() | infinity.
code_change(_, Time, State, _) ->
    handle(Time, State).

%% @private
-spec config_change(Spec, Time, State) ->
    {open, NState, infinity} |
    {closed, NState, NextTimeout} when
      Spec :: spec(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      NextTimeout :: integer() | infinity.
config_change(Spec, Time,
              #state{interval=PrevInterval, opens=Opens, map=Map} = State) ->
    Limit = sbroker_util:limit(Spec),
    Interval = sbroker_util:interval(Spec),
    {Min, Max} = sbroker_util:min_max(Spec),
    Diff = Interval - PrevInterval,
    Open = Time + Interval,
    {Overflow, NOpens, Next} = change(Map, Limit, Diff, Open, Min, Opens),
    NState = State#state{min=Min, max=Max, interval=Interval,
                         overflow=Overflow, opens=NOpens, open_next=Next},
    handle(Time, NState).

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
open_time(#state{map=Map, min=Min, small_time=Small, open_next=Next})
  when map_size(Map) < Min ->
    min(Small, Next);
open_time(#state{map=Map, max=Max, open_next=Next})
  when map_size(Map) < Max, is_integer(Next) ->
    Next;
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

change(Map, Limit, IntervalDiff, Open, Min, Opens) ->
    OverCapacity = max(0, map_size(Map) - Min),
    change(Limit-OverCapacity, IntervalDiff, Open, Opens).

change(OpenLen, IntervalDiff, Open, Opens) when OpenLen > 0 ->
    case queue:len(Opens) of
        PrevLen when PrevLen < OpenLen ->
            List = adjust_opens(IntervalDiff, queue:to_list(Opens)),
            New = lists:duplicate(OpenLen-PrevLen, Open),
            NOpens = queue:from_list(List ++ New),
            {0, NOpens, queue:get(NOpens)};
        PrevLen when PrevLen >= OpenLen ->
            List = queue:to_list(Opens),
            NList = adjust_opens(IntervalDiff, lists:sublist(List, OpenLen)),
            NOpens = queue:from_list(NList),
            {0, NOpens, queue:get(NOpens)}
    end;
change(NegOverflow, _, _, _) ->
    {-NegOverflow, queue:new(), infinity}.

adjust_opens(0, Opens)    -> Opens;
adjust_opens(Diff, Opens) -> [Open + Diff || Open <- Opens].

limit_continue(_, #state{overflow=0, open_next=infinity} = State) ->
    State;
limit_continue(Time,
               #state{overflow=0, interval=Interval, opens=Opens} = State) ->
    NOpens = queue:drop(queue:in(Time+Interval, Opens)),
    State#state{opens=NOpens, open_next=queue:get(NOpens)}.

limit_go(NMap, #state{overflow=0, opens=Opens} = State) ->
    NOpens = queue:drop(Opens),
    case queue:peek(NOpens) of
        {value, Next} ->
            State#state{opens=NOpens, open_next=Next, map=NMap};
        empty ->
            State#state{opens=NOpens, open_next=infinity, map=NMap}
    end.

limit_done(NMap, Time,
           #state{overflow=0, open_next=infinity, interval=Interval,
                  opens=Opens} = State) ->
    Next = Time+Interval,
    State#state{opens=queue:in(Next, Opens), open_next=Next, map=NMap};
limit_done(NMap, Time,
           #state{overflow=0, interval=Interval, opens=Opens} = State) ->
    State#state{opens=queue:in(Time+Interval, Opens), map=NMap};
limit_done(NMap, _, #state{overflow=Overflow} = State) ->
    State#state{overflow=Overflow-1, map=NMap}.

continue(Ref, Map, Size, Open, Time, ErrorState, OKState) ->
    case maps:find(Ref, Map) of
        {ok, _} ->
            {Status, NextTimeout} = status(Size, Time, OKState),
            {go, Open, Status, OKState, NextTimeout};
        error ->
            {Status, NextTimeout} = status(Size, Time, ErrorState),
            {error, Status, ErrorState, NextTimeout}
    end.

done(Ref, Map, Before, Time, #state{min=Min} = State) ->
    NMap = maps:remove(Ref, Map),
    case map_size(NMap) of
        Before ->
            NState = State#state{map=NMap},
            {Status, NextTimeout} = status(Before, Time, NState),
            {error, Status, NState, NextTimeout};
        _ when Before =:= Min ->
            demonitor(Ref, [flush]),
            NState = State#state{map=NMap, small_time=Time},
            {done, open, NState, infinity};
        _ when Before < Min ->
            demonitor(Ref, [flush]),
            {done, open, State#state{map=NMap}, infinity};
        After ->
            demonitor(Ref, [flush]),
            NState = limit_done(NMap, Time, State),
            {Status, NextTimeout} = status(After, Time, NState),
            {done, Status, NState, NextTimeout}
    end.

status(Size, _, #state{min=Min}) when Size < Min         -> {open, infinity};
status(Size, _, #state{max=Max}) when Size >= Max        -> {closed, infinity};
status(_, Time, #state{open_next=Next}) when Time < Next -> {closed, Next};
status(_, _, _)                                          -> {open, infinity}.

handle(_, #state{min=Min, map=Map} = State) when map_size(Map) < Min ->
    {open, State, infinity};
handle(_, #state{max=Max, map=Map} = State) when map_size(Map) >= Max ->
    {closed, State, infinity};
handle(Time, #state{open_next=Next} = State) when Time < Next ->
    {closed, State, Next};
handle(_, State) ->
    {open, State, infinity}.
