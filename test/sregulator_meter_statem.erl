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

-record(state, {queues, time, rand}).
-record(queue, {ask, interval, start_interval, rem_interval=0}).

module() ->
    sregulator_meter.

args() ->
    Seed = rand:export_seed_s(rand:seed_s(exsplus)),
    {regulators(), term_to_binary(Seed)}.

regulators() ->
    oneof([[{oneof([ask, ask_r]), choose(1, 5)}],
           [{ask, choose(1, 5)}, {ask_r, choose(1, 5)}],
           [{ask_r, choose(1, 5)}, {ask, choose(1, 5)}]]).

init(Time, {Queues, BinSeed}) ->
    NQueues = [#queue{ask=Ask, interval=sbroker_util:interval(Interval)} ||
               {Ask, Interval} <- Queues],
    Rand = rand:seed_s(binary_to_term(BinSeed)),
    State = #state{queues=NQueues, time=Time, rand=Rand},
    {State, timeout(State, Time)}.

update_next(State, Time, _, _, _, _) ->
    update_next(State, Time).

update_next(#state{time=PrevTime, rand=Rand, queues=Queues} = State, Time) ->
    Update = fun(#queue{rem_interval=Rem, interval=Interval} = Queue, NRand) ->
                     case Rem+PrevTime-Time of
                         NRem when NRem > 0 ->
                             {Queue#queue{rem_interval=NRem}, NRand};
                         _ ->
                             {NRem, NRand2} =
                                 sbroker_util:uniform_interval_s(Interval,
                                                                 NRand),
                             NQueue = Queue#queue{start_interval=Time,
                                                  rem_interval=NRem},
                            {NQueue, NRand2}
                     end
             end,
    {NQueues, NRand} = lists:mapfoldl(Update, Rand, Queues),
    NQueues2 = lists:keysort(#queue.rem_interval, lists:reverse(NQueues)),
    NState = State#state{time=Time, queues=NQueues2, rand=NRand},
    {NState, timeout(NState, Time)}.

update_post(State, Time, _, _, _, RelativeTime) ->
    update_post(State, Time, RelativeTime).

update_post(#state{time=PrevTime, queues=Queues} = State, Time, RelativeTime) ->
    Update = fun(#queue{rem_interval=Rem, ask=Ask}, Acc) ->
                     case Rem+PrevTime-Time of
                         NRem when NRem > 0 ->
                             true;
                         _ ->
                             Acc andalso update_post(Ask, RelativeTime)
                     end
             end,
    {_, Timeout} = update_next(State, Time),
    {lists:foldl(Update, true, Queues), Timeout}.

update_post(ask, RelativeTime) ->
    do_update_post(ask, RelativeTime);
update_post(ask_r, RelativeTime) ->
    do_update_post(ask_r, -RelativeTime).

do_update_post(Queue, ExpRelativeTime) ->
    receive
        {update, cast, ExpRelativeTime} ->
            true
    after
        0 ->
            flush_update_post(Queue, ExpRelativeTime)
    end.

flush_update_post(Queue, ExpRelativeTime) ->
    receive
        {update, cast, ObsRelativeTime} ->
            ct:pal("Relative Time update for ~p~nExpected: ~p~nObserved: ~p",
                   [Queue, ExpRelativeTime, ObsRelativeTime]),
            false
    after
        0 ->
            ct:pal("Did not receive update for ~p", [Queue]),
            false
    end.

change(#state{queues=OldQueues} = State, Time, {Queues, BinSeed}) ->
    NQueues = [#queue{ask=Ask, interval=sbroker_util:interval(Interval)} ||
               {Ask, Interval} <- Queues],
    Rand = rand:seed_s(binary_to_term(BinSeed)),
    Change = fun(#queue{ask=Ask, interval=Interval} = Queue, NRand) ->
                     case lists:keyfind(Ask, #queue.ask, OldQueues) of
                         #queue{start_interval=undefined} ->
                             {Queue, NRand};
                         #queue{start_interval=Start} ->
                             {Current, NRand2} =
                                sbroker_util:uniform_interval_s(Interval,
                                                                NRand),
                             Rem = max((Start+Current) - Time, 0),
                             NQueue = Queue#queue{start_interval=Start,
                                                  rem_interval=Rem},
                             {NQueue, NRand2};
                         false ->
                             {Queue, NRand}
                     end
             end,
    {NQueues2, NRand} = lists:mapfoldl(Change, Rand, NQueues),
    NQueues3 = lists:keysort(#queue.rem_interval, lists:reverse(NQueues2)),
    NState = State#state{queues=NQueues3, rand=NRand, time=Time},
    {NState, timeout(NState, Time)}.

timeout(#state{time=PrevTime, queues=Queues}, Time) ->
    MinRem = lists:foldl(fun(#queue{rem_interval=QRem}, AccRem) ->
                                 min(QRem, AccRem)
                         end, infinity, Queues),
    Time + max(MinRem + PrevTime - Time, 0).
