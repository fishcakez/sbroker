%%-------------------------------------------------------------------
%%
%% Copyright (c) 2015, James Fish <james@fishcakez.com>
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
-module(sbroker_alarm_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/2]).
-export([handle_update_next/3]).
-export([handle_update_post/3]).
-export([timeout_post/3]).
-export([change/3]).

-record(state, {target, interval, alarm, time, status, queue, interval_time}).

module() ->
    sbroker_alarm_meter.

args() ->
    {choose(0, 3),
     choose(1, 5),
     desc()}.

desc() ->
    oneof([a, b, c]).

init(Time, {Target, Interval, Alarm}) ->
    #state{target=Target, interval=Interval, alarm=Alarm, status=fast,
           time=Time, queue=fast, interval_time=0}.

handle_update_next(State, _, [Len, ProcessTime, Count, Time, _]) ->
    next(queue(Len, ProcessTime, Count, State), State, Time).

handle_update_post(State, Args, {_, Timeout} = Result) ->
    NState = handle_update_next(State, Result, Args),
    alarm_post(NState) andalso timeout_post(NState, Timeout).

timeout_post(#state{time=PrevTime, interval_time=IntervalTime} = State, Time,
             Timeout) ->
    NIntervalTime = IntervalTime-PrevTime+Time,
    timeout_post(State#state{time=Time, interval_time=NIntervalTime}, Timeout).

queue(Len, ProcessTime, Count, #state{target=Target}) ->
    if
        Len == 0 ->
            fast;
        Len * (ProcessTime div Count) < Target ->
            fast;
        true ->
            slow
    end.

next(Status, #state{status=Status, queue=Status} = State, Time) ->
    State#state{time=Time};
next(NStatus, #state{queue=NStatus, interval=Interval, time=PrevTime,
                     interval_time=IntervalTime} = State, Time) ->
    case IntervalTime-PrevTime+Time of
        NIntervalTime when NIntervalTime >= Interval ->
            State#state{status=NStatus, interval_time=0, time=Time};
        NIntervalTime ->
            State#state{interval_time=NIntervalTime, time=Time}
    end;
next(NStatus, #state{} = State, Time) ->
    State#state{queue=NStatus, interval_time=0, time=Time}.

timeout_post(#state{status=Status, queue=Status}, Timeout) ->
    case Timeout of
        infinity ->
            true;
        _ ->
            ct:pal("Timeout~nExpected: ~p~nObserved: ~p", [infinity, Timeout]),
            false
    end;
timeout_post(#state{time=Time, interval=Interval, interval_time=IntervalTime},
             Timeout) ->
    case Time + max(0, Interval - IntervalTime) of
        Timeout ->
            true;
        Expected ->
            ct:pal("Timeout~nExpected: ~p~nObserved: ~p", [Expected, Timeout]),
            false
    end.

alarm_post(#state{status=fast, alarm=Alarm}) ->
    case sbroker_test_handler:get_alarms() of
        [] ->
            true;
        [{Alarm, {message_queue_slow, Pid}}] when Pid == self() ->
            ct:pal("Alarm set"),
            false
    end;
alarm_post(#state{status=slow, alarm=Alarm}) ->
    case sbroker_test_handler:get_alarms() of
        [] ->
            ct:pal("Alarm clear"),
            false;
        [{Alarm, {message_queue_slow, Pid}}] when Pid == self() ->
            true
    end.

change(#state{alarm=Alarm, interval_time=IntervalTime, time=PrevTime} = State,
       Time, {Target, Interval, Alarm}) ->
    NIntervalTime = IntervalTime - PrevTime + Time,
    State#state{target=Target, interval=Interval, time=Time,
                interval_time=NIntervalTime};
change(_, Time, Args) ->
    init(Time, Args).
