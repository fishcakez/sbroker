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
-module(sbroker_statem_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([time_dependence/1]).
-export([init/1]).
-export([handle_timeout/3]).
-export([handle_out/3]).
-export([handle_out_r/3]).
-export([config_change/3]).

module() ->
    oneof([sbroker_statem_queue, sbroker_statem2_queue]).

args() ->
    {oneof([out, out_r]), resize(4, list(oneof([0, choose(1, 2)])))}.

init({Out, Drops}) ->
    {Out, drop, 0, infinity, {Drops, Drops}}.

time_dependence({_, _}) ->
    independent.

handle_timeout(_, _, {[], []} = State) ->
    {0, State};
handle_timeout(_, [], State) ->
    {0, State};
handle_timeout(Time, L, {[], Config}) ->
    handle_timeout(Time, L, {Config, Config});
handle_timeout(_, L, {[Drop | Drops], Config}) ->
    {min(Drop, length(L)), {Drops, Config}}.

handle_out(Time, L, State) ->
    do_handle_out(Time, L, State).

handle_out_r(Time, L, State) ->
    do_handle_out(Time, L, State).

config_change(_, {Out, Config}, {_, Config} = State) ->
    {Out, drop, 0, infinity, State};
config_change(_, {Out, Drops}, _) ->
    {Out, drop, 0, infinity, {Drops, Drops}}.

%% Internal

do_handle_out(Time, L, State) ->
    case handle_timeout(Time, L, State) of
        {Drop, {_, Config}} when Drop == length(L) ->
            {Drop, {Config, Config}};
        NonEmpty ->
            NonEmpty
    end.
