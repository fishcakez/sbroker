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
-module(sregulator_codel_valve_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/3]).
-export([handle_update/3]).
-export([handle_ask/2]).
-export([handle_done/2]).
-export([handle/2]).
-export([config_change/4]).

-record(state, {target, interval, count=0, open_next=undefined,
                first_above_time=undefined, opening=false, updated=false,
                now=undefined}).

module() ->
    sregulator_codel_valve.

args() ->
    ?LET({Min, Max},
         ?SUCHTHAT({Min, Max}, {choose(0, 5), oneof([choose(0, 5), infinity])},
                   Min =< Max),
         {choose(-10, 10), choose(1, 3), Min, Max}).

init({Target, Interval, Min, Max}, _, _) ->
    NTarget = sbroker_util:relative_target(Target),
    NInterval = sbroker_util:interval(Interval),
    {Min, Max, closed, #state{target=NTarget, interval=NInterval}}.

handle_update(Value, Time, State) ->
    NState = State#state{now=Time, updated=true},
    {Status, NState2} = do_update(Value, NState),
    case NState2#state.opening of
        true ->
            update_opening(Status, NState2);
        false ->
            update_not_opening(Status, NState2)
    end.

handle_ask(Time, State) ->
    handle_ask(State#state{now=Time}).

handle_done(Time, State) ->
    handle(Time, State).

handle(Time, State) ->
    handle(State#state{now=Time}).

config_change({Target, Interval, Min, Max}, _, Time,
              #state{first_above_time=FirstAbove,
                     open_next=OpenNext} = State) ->
    NTarget = sbroker_util:relative_target(Target),
    NInterval = sbroker_util:interval(Interval),
    NFirstAbove = reduce(FirstAbove, Time+NInterval),
    NOpenNext = reduce(OpenNext, Time+NInterval),
    NState = State#state{target=NTarget, interval=NInterval,
                         first_above_time=NFirstAbove, open_next=NOpenNext,
                         now=Time},
    {Status, NState} = handle(NState),
    {Min, Max, Status, NState}.

do_update(Value, #state{target=Target} = State) when Value >= Target ->
    {closed, State#state{first_above_time=undefined}};
do_update(_, #state{first_above_time=undefined, interval=Interval,
                        now=Now} = State) ->
    {closed, State#state{first_above_time=Now+Interval}};
do_update(_, #state{first_above_time=FirstAbove, now=Now} = State)
  when Now >= FirstAbove ->
    {open, State};
do_update(_, State) ->
    {closed, State}.

update_opening(closed, State) ->
    {closed, State#state{opening=false}};
update_opening(open, #state{now=Now, open_next=OpenNext} = State)
  when Now >= OpenNext ->
    {open, State};
update_opening(open, State) ->
    {closed, State}.

update_not_opening(open, State) ->
    {open, State};
update_not_opening(closed, State) ->
    {closed, State}.

handle_ask(#state{opening=true, now=Now, open_next=OpenNext,
                  count=Count} = State)
  when Now >= OpenNext ->
    handle(control_law(OpenNext, State#state{count=Count+1}));
handle_ask(#state{opening=false, now=Now, first_above_time=FirstAbove,
                  count=Count, open_next=OpenNext, interval=Interval} = State)
  when Now >= FirstAbove ->
    NState = State#state{opening=true},
    NCount = if
                 Count > 2 andalso Now - OpenNext < 8 * Interval ->
                     Count - 2;
                 true ->
                     1
             end,
    handle(control_law(Now, NState#state{count=NCount})).

handle(#state{first_above_time=undefined} = State) ->
    {closed, State};
handle(#state{opening=false, now=Now, first_above_time=FirstAbove} = State)
  when Now >= FirstAbove ->
    {open, State};
handle(#state{updated=false} = State) ->
    {closed, State};
handle(#state{opening=true, now=Now, open_next=OpenNext} = State)
  when Now >= OpenNext ->
    {open, State};
handle(State) ->
    {closed, State}.

control_law(Start, #state{interval=Interval, count=Count} = State) ->
    OpenNext = Start + erlang:trunc(Interval / math:sqrt(Count)),
    State#state{open_next=OpenNext, updated=false}.

reduce(undefined, _) ->
    undefined;
reduce(Next, Min) ->
    min(Next, Min).
