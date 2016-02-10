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
-module(sbroker_drop_statem).

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
    sbroker_drop_queue.

args() ->
    {oneof([out, out_r]),
     oneof([drop, drop_r]),
     oneof([choose(0, 5), infinity])}.

time_dependence(undefined) ->
    independent.

init({Out, Drop, Max}) ->
    {Out, Drop, Max, undefined}.

handle_timeout(_, _, State) ->
    {0, State}.

handle_out(_, _, State) ->
    {0, State}.

handle_out_r(_, _, State) ->
    {0, State}.

config_change(_, {Out, Drop, Max}, undefined) ->
    {Out, Drop, Max, undefined}.
