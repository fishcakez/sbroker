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
-module(sbroker_timeout_meter_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/2]).
-export([handle_update_next/3]).
-export([handle_update_post/3]).
-export([timeout_post/3]).
-export([change/3]).

module() ->
    sbroker_timeout_meter.

args() ->
    oneof([choose(0, 3), infinity]).

init(_, Timeout) ->
    Timeout.

handle_update_next(Timeout, _, _) ->
    Timeout.

handle_update_post(Timeout, [_, _, _, Time, _], {_, Timeout2}) ->
    timeout_post(Timeout, Time, Timeout2).

timeout_post(infinity, _, Timeout) ->
    case Timeout of
        infinity ->
            true;
        _ ->
            ct:pal("Timeout~nExpected: ~p~nObserved: ~p", [infinity, Timeout]),
            false
    end;
timeout_post(Timeout, Time, Timeout2) ->
    case Time + Timeout of
        Timeout2 ->
            true;
        Timeout3 ->
            ct:pal("Timeout~nExpected: ~p~nObserved: ~p", [Timeout3, Timeout2]),
            false
    end.

change(_, _, Timeout) ->
    Timeout.
