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
%% @doc Updates a list of regulators with the relative time of a queue
%% after a random interval. Regulators are updated with `sregulator:cast/2' and
%% any errors sending the update are ignored.
%%
%% `sregulator_meter' can be used as the `sbroker_meter' in a `sbroker' or
%% a `sregulator'. Its argument is of the form:
%% ```
%% {[{Regulator :: sregulator:regulator, Queue :: ask | ask_r}, ...}],
%%  Interval :: pos_integer()}.
%% ```
%%
%% `Regulator' is a regulator process to update with the approximate relative
%% time of queue `Queue'. The list of regulators is updated randomly using with
%% intervals uniformly distributed from `0.5 * Interval' to `1.5 * Interval'
%% milliseconds. This random interval is used to prevent synchronisation of
%% update messages or their side effects, see reference.
%%
%% @see sregulator
%% @reference Sally Floyd and Van Jacobson, The Synchronization of Periodic
%% Routing Messages, 1994.
-module(sregulator_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/5]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([terminate/2]).

-record(state, {regulators :: [{sregulator:regulator(), ask | ask_r}, ...],
                interval :: pos_integer(),
                rand :: sbroker_rand:state(),
                current_interval :: pos_integer(),
                update_next :: integer()}).

%% @private
-spec init(Time, {Regulators, Interval} | {Regulators, Interval, Seed}) ->
    {State, UpdateNext} when
      Time :: integer(),
      Regulators :: [{sregulator:regulator(), ask | ask_r}, ...],
      Interval :: pos_integer(),
      Seed :: sbroker_rand:seed(),
      State :: #state{},
      UpdateNext :: integer().
init(Time, {Regulators, Interval}) ->
    Seed = sbroker_rand:export_seed_s(sbroker_rand:seed_s()),
    init(Time, {Regulators, Interval, Seed});
init(Time, {Regulators, Interval, Seed}) ->
    NRegulators = regulators(Regulators),
    NInterval = sbroker_util:interval(Interval),
    Rand = sbroker_rand:seed_s(Seed),
    {Current, NRand} = sbroker_rand:uniform_interval_s(NInterval, Rand),
    UpdateNext = Time + Current,
    State = #state{regulators=NRegulators, interval=NInterval, rand=NRand,
                   current_interval=Current, update_next=UpdateNext},
    {State, UpdateNext}.

%% @private
-spec handle_update(QueueDelay, ProcessDelay, RelativeTime, Time, State) ->
    {NState, UpdateNext} when
      QueueDelay :: non_neg_integer(),
      ProcessDelay :: non_neg_integer(),
      RelativeTime :: integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      UpdateNext :: integer().
handle_update(_, _, _, Time, #state{update_next=UpdateNext} = State)
  when Time < UpdateNext ->
    {State, UpdateNext};
handle_update(_, _, RelativeTime, Time,
              #state{regulators=Regulators, rand=Rand,
                     interval=Interval} = State) ->
    _ = [update(Regulator, Queue, RelativeTime) ||
         {Regulator, Queue} <- Regulators],
    {Current, NRand} = sbroker_rand:uniform_interval_s(Interval, Rand),
    UpdateNext = Time + Current,
    NState = State#state{rand=NRand, current_interval=Current,
                         update_next=UpdateNext},
    {NState, UpdateNext}.

%% @private
-spec handle_info(Msg, Time, State) -> {State, UpdateNext} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      UpdateNext :: integer().
handle_info(_, Time, State) ->
    handle(Time, State).

handle(Time, #state{update_next=UpdateNext} = State) ->
    {State, max(Time, UpdateNext)}.

%% @private
-spec code_change(OldVsn, Time, State, Extra) -> {State, UpdateNext} when
      OldVsn :: any(),
      Time :: integer(),
      State :: #state{},
      Extra :: any(),
      UpdateNext :: integer().
code_change(_, Time, State, _) ->
    handle(Time, State).

%% @private
-spec config_change({Regulators, Interval} | {Regulators, Interval, Seed}, Time,
                    State) ->
    {NState, UpdateNext} when
      Time :: integer(),
      Regulators :: [{sregulator:regulator(), ask | ask_r}, ...],
      Interval :: pos_integer(),
      Seed :: sbroker_rand:seed(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      UpdateNext :: integer().
config_change({Regulators, Interval}, Time, #state{rand=Rand} = State) ->
    Seed = sbroker_rand:export_seed_s(Rand),
    config_change({Regulators, Interval, Seed}, Time, State);
config_change({Regulators, Interval, Seed}, Time,
              #state{current_interval=Current, update_next=UpdateNext}) ->
    NRegulators = regulators(Regulators),
    NInterval = sbroker_util:interval(Interval),
    LastUpdate = UpdateNext - Current,
    NCurrent = min(max(Current, NInterval div 2), NInterval + NInterval div 2),
    NUpdateNext = LastUpdate + NCurrent,
    NRand = sbroker_rand:seed_s(Seed),
    NState = #state{regulators=NRegulators, interval=NInterval,
                    current_interval=NCurrent, update_next=NUpdateNext,
                    rand=NRand},
    {NState, max(Time, NUpdateNext)}.

%% @private
-spec terminate(Reason, State) -> ok when
      Reason :: any(),
      State :: #state{}.
terminate(_, _) ->
    ok.

%% Internal

regulators(Regulators) when length(Regulators) > 0 ->
    case lists:all(fun is_regulator/1, Regulators) of
        true ->
            Regulators;
        false ->
            error(badarg, [Regulators])
    end;
regulators(Other) ->
    error(badarg, [Other]).

is_regulator({Process, ask}) ->
    is_process(Process);
is_regulator({Process, ask_r}) ->
    is_process(Process);
is_regulator(_) ->
    false.

is_process(Pid) when is_pid(Pid) ->
    true;
is_process(Name) when is_atom(Name) ->
    true;
is_process({Name, Node}) when is_atom(Name), is_atom(Node) ->
    true;
is_process({global, _}) ->
    true;
is_process({via, Mod, _}) when is_atom(Mod) ->
    true;
is_process(_) ->
    false.

update(Regulator, ask, RelativeTime) ->
    cast(Regulator, RelativeTime);
update(Regulator, ask_r, RelativeTime) ->
    cast(Regulator, -RelativeTime).

cast(Regulator, RelativeTime) ->
    try
        sregulator:cast(Regulator, RelativeTime)
    catch
        _:_ ->
            ok
    end.
