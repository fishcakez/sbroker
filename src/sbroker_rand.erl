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
%% @private
-module(sbroker_rand).

%% public API

-export([seed_s/0]).
-export([seed_s/1]).
-export([export_seed_s/1]).
-export([uniform_interval_s/2]).

%% types

-type state() ::
    {rand, rand:state()} | {random, {integer(), integer(), integer()}}.

-type seed() ::
    {rand, rand:export_seed()} | {random, {integer(), integer(), integer()}}.

-export_type([state/0]).
-export_type([seed/0]).

%% public API

-spec seed_s() -> State when
      State :: state().
seed_s() ->
    try rand:seed_s(exsplus) of
        RandState ->
            {rand, RandState}
    catch
        error:undef ->
            {A, B, _} = os:timestamp(),
            C = erlang:phash2({self(), make_ref()}),
            {random, {A, B, C}}
    end.

-spec seed_s(Seed) -> State when
      Seed :: seed(),
      State :: state().
seed_s({rand, RandSeed}) ->
    {rand, rand:seed_s(RandSeed)};
seed_s({random, _} = State) ->
    State.

-spec export_seed_s(State) -> Seed when
      State :: state(),
      Seed :: seed().
export_seed_s({rand, RandState}) ->
    {rand, rand:export_seed_s(RandState)};
export_seed_s({random, _} = Seed) ->
    Seed.

-spec uniform_interval_s(Interval, State) -> {CurrentInterval, NState} when
      Interval :: pos_integer(),
      State :: state(),
      CurrentInterval :: pos_integer(),
      NState :: state().
uniform_interval_s(Interval, {RandMod, RandState}) ->
    {Int, NRandState} = RandMod:uniform_s(Interval, RandState),
    {(Interval div 2) + Int, {RandMod, NRandState}}.
