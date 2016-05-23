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
-module(sregulator_meter_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/2]).
-export([update_next/6]).
-export([update_post/6]).
-export([change/3]).
-export([timeout/2]).

-record(state, {interval, queue, time, current_interval, rem_interval, rand}).

module() ->
    sregulator_meter.

args() ->
    Seed = sbroker_rand:export_seed_s(sbroker_rand:seed_s()),
    {oneof([ask, ask_r]), choose(1, 5), term_to_binary(Seed)}.

init(Time, {Queue, Interval, BinSeed}) ->
    NInterval = sbroker_util:interval(Interval),
    Rand = sbroker_rand:seed_s(binary_to_term(BinSeed)),
    {Rem, NRand} = sbroker_rand:uniform_interval_s(NInterval, Rand),
    State = #state{interval=NInterval, queue=Queue, time=Time, rand=NRand,
                   rem_interval=Rem, current_interval=Rem},
    {State, timeout(State, Time)}.

update_next(#state{time=PrevTime, rem_interval=Rem} = State, Time, _, _, _, _)
  when Rem + PrevTime - Time > 0 ->
    NState = State#state{time=Time, rem_interval=Rem+PrevTime-Time},
    {NState, timeout(NState, Time)};
update_next(#state{rand=Rand, interval=Interval} = State, Time, _, _, _, _) ->
    {Rem, NRand} = sbroker_rand:uniform_interval_s(Interval, Rand),
    NState = State#state{time=Time, rem_interval=Rem, current_interval=Rem,
                         rand=NRand},
    {NState, timeout(NState, Time)}.

update_post(#state{time=PrevTime, rem_interval=Rem} = State, Time, _, _, _, _)
  when Rem + PrevTime - Time > 0 ->
    NState = State#state{time=Time, rem_interval=Rem+PrevTime-Time},
    {true, timeout(NState, Time)};
update_post(#state{rand=Rand, interval=Interval, queue=Queue} = State, Time, _,
            _, _, RelativeTime) ->
    {Rem, NRand} = sbroker_rand:uniform_interval_s(Interval, Rand),
    NState = State#state{time=Time, rem_interval=Rem, current_interval=Rem,
                         rand=NRand},
    {update_post(Queue, RelativeTime), timeout(NState, Time)}.

update_post(ask, RelativeTime) ->
    update_post(RelativeTime);
update_post(ask_r, RelativeTime) ->
    update_post(-RelativeTime).

update_post(ExpRelativeTime) ->
    receive
        {update, cast, ExpRelativeTime} ->
            true;
        {update, cast, ObsRelativeTime} ->
            ct:pal("Relative Time update~nExpected: ~p~nObserved: ~p",
                   [ExpRelativeTime, ObsRelativeTime]),
            false
    after
        0 ->
            ct:pal("Did not receive update"),
            false
    end.

change(#state{current_interval=Current, rem_interval=Rem,
              time=PrevTime} = State, Time, {Queue, Interval, BinSeed}) ->
    NInterval = sbroker_util:interval(Interval),
    MinInterval = NInterval div 2,
    MaxInterval = MinInterval + NInterval,
    NCurrent = min(max(Current, MinInterval), MaxInterval),
    Rem2 = Rem + PrevTime - Time,
    NRem = Rem2 + NCurrent - Current,
    NRand = sbroker_rand:seed_s(binary_to_term(BinSeed)),
    NState = State#state{interval=NInterval, queue=Queue, time=Time,
                         current_interval=NCurrent, rem_interval=NRem,
                         rand=NRand},
    {NState, timeout(NState, Time)}.

timeout(#state{time=PrevTime, rem_interval=Rem}, Time) ->
    Time + max(Rem + PrevTime - Time, 0).
