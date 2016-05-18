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
%% @doc Implements a valve which increases its size based on decreasing
%% intervals of updates being below a target between a minimum and maximum
%% capacity.
%%
%% `sregulator_codel_value' can be used as the `sregulator_valve' in a
%% `sregulator'. Its argument is of the form:
%% ```
%% {Target :: integer(), Interval :: pos_integer(), Min :: non_neg_integer(),
%%  Max :: non_neg_integer() | infinity}
%% '''
%% `Target' is the target relative value in milliseconds. `Interval' is the
%% initial interval in milliseconds. The valve will open when updates remain
%% below the target (in `native' time units) for an interval. Each consequeitive
%% is smaller than the last until an update above the target is received. The
%% valve is always open when below the minimum and always closed once it reaches
%% the maximum.
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
%% The algorithm used in this valve is similar to `sbroker_codel_queue', except
%% designed to keep the relevant queue slow (but not too slow) instead of fast.
%% Therefore trying to ensure the counter party to the queue is always fast
%% without using too many resources. This works by increasing the concurrency or
%% number of requests when the queue is consistently fast and remaining static
%% when the queue is slow. Therefore forcing the queue to be slightly slow.
%%
%% This valve is designed to be used a `sbroker_codel_queue' with the same
%% `Interval' and `Target's that are between 10% and 20% of the `Interval'. The
%% target range is suggested due to the observation in the CoDel paper that the
%% queue goes from to fast to slow over this target range. Higher targets result
%% in heavily congested queues and wasted resources. A suggested initial
%% `Interval' is the 95% percentile of the time it takes to stop a task and
%% restart, as this is equivalent to the round trip when a packet is dropped and
%% resent to rejoin the queue in the paper.
%%
%% Decreasing the target of the valve makes it more resistant to bursts and
%% reducing the target of the queue will increase the rate of shrinking when
%% load decreases. This fulfils the general desire to increase resouce usage as
%% late as possible and decrease resource usage as early as possible. If the
%% queue's target is significantly lower than valve's this may lead to churn as
%% the queue and valve may act against each other. Also if the minimum is too
%% high the queue may drop the requests only for the valve to allow immediate
%% enqueues.
%%
%% More investigation needs to be done on suitable parameters.
%% @reference Kathleen Nichols and Van Jacobson, Controlling Queue Delay,
%% ACM Queue, 6th May 2012.
-module(sregulator_codel_valve).

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

-record(state, {min :: non_neg_integer(),
                max :: non_neg_integer() | infinity,
                target :: integer(),
                interval :: pos_integer(),
                count=0 :: non_neg_integer(),
                open_next :: integer(),
                open_first=infinity :: integer() | infinity | opening | await,
                map :: sregulator_valve:internal_map()}).

%% sregulator_valve api

%% @private
-spec init(Map, Time, {RelativeTarget, Interval, Min, Max}) ->
    {open | closed, State} when
      Map :: sregulator_valve:internal_map(),
      Time :: integer(),
      RelativeTarget :: integer(),
      Interval :: pos_integer(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      State :: #state{}.
init(Map, Time, {RelativeTarget, Interval, Min, Max}) ->
    {Min, Max} = sbroker_util:min_max(Min, Max),
    State = #state{min=Min, max=Max,
                   target=sbroker_util:relative_target(RelativeTarget),
                   interval=sbroker_util:interval(Interval), open_next=Time,
                   map=Map},
    handle(Time, State).

%% @private
-spec handle_ask(Pid, Ref, Time, State) -> {open | closed, NState} when
      Pid :: pid(),
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.

handle_ask(Pid, Ref, Time,
           #state{min=Min, open_first=First, open_next=Next,
                  count=C, map=Map} = State) ->
    NMap = maps:put(Ref, Pid, Map),
    NState = State#state{map=NMap},
    if
        map_size(NMap) < Min ->
            {open, NState};
        %% open based on size of Min-1
        map_size(NMap) =:= Min ->
            handle(Time, NState);
        %% opening and fast for a consecutive interval
        First == opening, Time >= Next->
            handle(Time, open_control(C+1, Next, NState));
        %% fast for an initial interval
        Time >= First ->
            handle(Time, open_control(Time, NState))
    end.

%% @private
-spec handle_done(Ref, Time, State) ->
    {done | error, open | closed, NState} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_done(Ref, Time, #state{map=Map} = State) ->
    Before = map_size(Map),
    done(Ref, Map, Before, Time, State).

%% @private
-spec handle_continue(Ref, Time, State) ->
    {continue | done | error, open | closed, NState} when
      Ref :: reference(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_continue(Ref, Time,
                #state{min=Min, max=Max, open_first=First, open_next=Next,
                       count=C, map=Map} = State) ->
  Size = map_size(Map),
  if
      Size < Min ->
          continue(Ref, Map, Size, Time, State, State);
      Size =:= Min ->
          continue(Ref, Map, Size, Time, State, State);
      Size > Max ->
          done(Ref, Map, Size, Time, State);
      First == opening, Time >= Next ->
          NState = open_control(C+1, Next, State),
          continue(Ref, Map, Size, Time, State, NState);
      is_integer(First), Time >= First ->
          NState = open_control(Time, State),
          continue(Ref, Map, Size, Time, State, NState);
      true ->
          done(Ref, Map, Size, Time, State)
  end.

%% @private
-spec handle_update(Value, Time, State) -> {open | closed, NState} when
      Value :: integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_update(RelativeTime, Time,
              #state{open_first=infinity, target=Target,
                     interval=Interval} = State) when RelativeTime < Target ->
    handle(Time, State#state{open_first=Time+Interval});
handle_update(RelativeTime, Time,
              #state{target=Target, open_first=await} = State)
  when RelativeTime < Target ->
    handle(Time, State#state{open_first=opening});
handle_update(RelativeTime, Time, #state{target=Target} = State)
  when RelativeTime < Target ->
    handle(Time, State);
handle_update(_, Time, #state{open_first=infinity} = State) ->
    handle(Time, State);
handle_update(_, Time, State) ->
    handle(Time, State#state{open_first=infinity}).

%% @private
-spec handle_info(Msg, Time, State) -> {open | closed, NState} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
handle_info({'DOWN', Ref, process, _, _}, Time, #state{map=Map} = State) ->
    NMap = maps:remove(Ref, Map),
    handle(Time, State#state{map=NMap});
handle_info(_, Time, State) ->
    handle(Time, State).

%% @private
-spec config_change({RelativeTarget, Interval, Min, Max}, Time, State) ->
    {open | closed, NState} when
      RelativeTarget :: integer(),
      Interval :: pos_integer(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      Time :: integer(),
      State :: #state{},
      NState :: #state{}.
config_change({RelativeTarget, Interval, Min, Max}, Time, State) ->
    {Min, Max} = sbroker_util:min_max(Min, Max),
    NState = State#state{min=Min, max=Max,
                         target=sbroker_util:relative_target(RelativeTarget),
                         interval=sbroker_util:interval(Interval)},
    change(Time, NState).

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

handle(Time, #state{map=Map} = State) ->
    {status(map_size(Map), Time, State), State}.

status(Size, _, #state{min=Min}) when Size < Min ->
    open;
status(Size, _, #state{max=Max}) when Size >= Max ->
    closed;
status(_, _, #state{open_first=infinity}) ->
    closed;
status(_, _, #state{open_first=await}) ->
    closed;
status(_, Time, #state{open_first=opening, open_next=Next}) when Time < Next ->
    closed;
status(_, _, #state{open_first=opening}) ->
    open;
status(_, Time, #state{open_first=First}) when Time < First ->
    closed;
status(_, _, _) ->
    open.

%% If first fast update in fast interval was "soon" after switching from
%% opening to closed use the previous dropping interval length as it
%% should be appropriate.
open_control(Time, #state{interval=Interval, count=C, open_next=Next} = State)
  when C > 2 andalso Time - Next < 8 * Interval ->
    open_control(C - 2, Time, State);
open_control(Time, #state{interval=Interval} = State) ->
    State#state{count=1, open_next=Time+Interval, open_first=await}.

%% Shrink the interval to increase open rate and reduce relative time.
open_control(C, Time, #state{interval=Interval} = State) ->
    Next = Time + trunc(Interval / math:sqrt(C)),
    State#state{count=C, open_first=await, open_next=Next}.

continue(Ref, Map, Size, Time, ErrorState, OKState) ->
    case maps:find(Ref, Map) of
        {ok, _} ->
            {continue, status(Size, Time, OKState), OKState};
        error ->
            {error, status(Size, Time, ErrorState), ErrorState}
    end.

done(Ref, Map, Before, Time, State) ->
    NMap = maps:remove(Ref, Map),
    NState = State#state{map=NMap},
    case map_size(NMap) of
        Before ->
            {error, status(Before, Time, NState), NState};
        After ->
            demonitor(Ref, [flush]),
            {done, status(After, Time, NState), NState}
    end.

change(Time, #state{open_first=First, interval=Interval} = State)
  when is_integer(First), First > Time+Interval ->
    change(Time, State#state{open_first=Time+Interval});
change(Time, #state{open_next=Next, interval=Interval} = State)
  when is_integer(Next) andalso Next > Time+Interval ->
    change(Time, State#state{open_next=Time+Interval});
change(Time, State) ->
    handle(Time, State).
