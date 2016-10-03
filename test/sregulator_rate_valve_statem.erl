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
-module(sregulator_rate_valve_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/3]).
-export([handle_ask/2]).
-export([handle_done/2]).
-export([handle/2]).
-export([timeout/2]).
-export([config_change/4]).

-record(state, {interval, intervals, overflow, active, time}).

module() ->
    sregulator_rate_valve.

args() ->
    ?LET({Min, Max},
         ?SUCHTHAT({Min, Max}, {choose(0, 5), oneof([choose(0, 5), infinity])},
                   Min =< Max),
         {choose(0, 5), choose(1, 5), Min, Max}).

init({Limit, Interval, Min, Max}, Size, Time) ->
    NInterval = erlang:convert_time_unit(Interval, milli_seconds, native),
    case max(0, Size - (Min + Limit)) of
        Overflow when Overflow > 0 ->
            State = #state{interval=NInterval, intervals=[], active=Limit,
                           overflow=Overflow},
            {Min, Max, closed, State};
        0 ->
            Active = max(0, Size - Min),
            Intervals = lists:duplicate(Limit-Active, NInterval),
            State = #state{interval=NInterval, intervals=Intervals,
                           active=Active, overflow=0, time=Time},
            {Status, NState} = handle(Time, State),
            {Min, Max, Status, NState}
    end.

handle_ask(Time, State) ->
    {open, NState} = handle(Time, State),
    #state{intervals=[_|Intervals], active=Active} = NState,
    handle(State#state{intervals=Intervals, active=Active+1}).

handle_done(Time, #state{overflow=Overflow} = State) when Overflow > 0 ->
    handle(Time, State#state{overflow=Overflow-1});
handle_done(Time, #state{overflow=0, active=Active} = State)
  when Active > 0 ->
    {_, #state{intervals=Intervals} = NState} = handle(Time, State),
    handle(NState#state{active=Active-1, intervals=Intervals++[0]});
handle_done(_, State) ->
    {error, State}.

handle(Time, #state{intervals=Intervals, time=PrevTime} = State) ->
    NIntervals = [Interval + (Time-PrevTime) || Interval <- Intervals],
    handle(State#state{intervals=NIntervals, time=Time}).

timeout(Time, State) ->
    {_, NState} = handle(Time, State),
    case NState of
        #state{overflow=0, intervals=[Max | _], interval=Interval}
          when Max < Interval ->
            Time + (Interval - Max);
        #state{} ->
            infinity
    end.

config_change({Limit, Interval, Min, Max}, Size, Time, State) ->
    {_, #state{intervals=Intervals}} = handle(Time, State),
    NInterval = erlang:convert_time_unit(Interval, milli_seconds, native),
    Active = max(0, Size - Min),
    case Limit - Active of
        NegOverflow when NegOverflow < 0 ->
            NState = #state{interval=NInterval, intervals=[],
                            overflow=-NegOverflow, active=Limit, time=Time},
            {Min, Max, closed, NState};
        Slots when length(Intervals) >= Slots ->
            NIntervals = lists:sublist(Intervals, Slots),
            NState = #state{interval=NInterval, intervals=NIntervals,
                            active=Active, overflow=0, time=Time},
            {Status, NState2} = handle(NState),
            {Min, Max, Status, NState2};
        Slots when length(Intervals) < Slots ->
            Pred = fun(Interval2) -> Interval2 > NInterval end,
            {Long, Short} = lists:splitwith(Pred, Intervals),
            Ready = lists:duplicate(Slots-length(Intervals), NInterval),
            NIntervals = Long ++ Ready ++ Short,
            NState = #state{interval=NInterval, intervals=NIntervals,
                            active=Active, overflow=0, time=Time},
            {Min, Max, open, NState}
    end.

%% Internal

handle(#state{overflow=0, interval=Interval, intervals=[Max|_]} = NState)
  when Max >= Interval ->
    {open, NState};
handle(NState) ->
    {closed, NState}.
